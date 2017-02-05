package spark.app

import java.net.URI

import com.datastax.driver.core.Session
import com.datastax.spark.connector.cql.CassandraConnector
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import com.typesafe.config.{Config, ConfigFactory}
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkConf, SparkContext}
import org.joda.time.DateTime
import org.rogach.scallop.{ScallopConf, ScallopOption}
import org.slf4j.LoggerFactory
import org.spark_project.guava.util.concurrent.RateLimiter
import spark.cassandra.Connector
import spark.model.Identity.{CoreFields, IdentityPayload}
import spark.model.{RawIdentityEvent, UniqueFields}
import spark.processor.UniqueUpmIdProcessor
import spark.util.ScalaExtensions

import scala.util.Random

/**
	*
	* @author Adam Martini
	*/
object CassandraDynamicExtractionPOC {
	private val log = LoggerFactory.getLogger(getClass)
	private val mapper = new ObjectMapper()
	private val random = new Random()
	private val mockEventsKeyspace = "test"
	private val mockEventsTable = "event"
	private val mockEventsKeyspaceTableMap = Map(
		"keyspace" -> mockEventsKeyspace,
		"table" -> mockEventsTable
	)

	def main(args: Array[String]): Unit = {
		// parse optional arguments
		val params = new CassandraDynamicExtractionAppParams(args)
		val env = params.env.getOrElse("processor-test")
		val host = params.host.getOrElse(Connector.host)
		val port = params.port.getOrElse(Connector.port)
		val numUniqueUsers = params.numUniqueUsers.toOption.get
		val eventsPerSecond = params.eventsPerSecond.toOption.get

		// load appropriate config
		val config: Config = ConfigFactory.load(env)
		val numCassandraNodes = config.getInt("cassandra.cluster.nodes.count")
		val processorS3Bucket = config.getString("processor.s3.bucket")

		// setup spark context
		val conf: SparkConf = new SparkConf()
			.setAppName("CassandraConnectorPOC")
			.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
			.registerKryoClasses(Array(classOf[RawIdentityEvent], classOf[UniqueFields]))
			.set("spark.cassandra.connection.host", host)
			.set("spark.cassandra.connection.port", port.toString)
			.set("spark.yarn.executor.memoryOverhead", "10G")
		val sc = new SparkContext(conf)
		val cc = CassandraConnector(sc.getConf)
		val broadcastMapper = sc.broadcast(mapper)

		// setup sql context
		val sqlContext = SparkSession
			.builder()
			.getOrCreate()
		import sqlContext.implicits._ // required for implicit Encoders

		// cleanup test bucket
		clearS3Data(sc, processorS3Bucket)

		// db setup
		cc.withSessionDo(session => {
			createMockEventsKeyspace(session, numCassandraNodes)
			createMockEventsTable(session)
			truncateMockEventsTable(session)
		})

		// generate seed events
		val idRanges = ScalaExtensions.splitRange(
			Range(0, numUniqueUsers),
			(numUniqueUsers / 50000) + 1
		)
		val eventRange = Range(0, random.nextInt(5) + 1)
		var numEvents = 0l
		idRanges.foreach(idRange => {
			val seedEvents = idRange.flatMap(i => {
				eventRange.map(_ => {
					IdentityPayload[CoreFields](
						id = upmId(i),
						time = IdentityPayload.time(randomTime),
						set = CoreFields(
							Some(upmId(i))
						)
					)
				})
			})
				.map(payload => {
					broadcastMapper.value.registerModule(DefaultScalaModule)
					RawIdentityEvent.from(
						payload,
						broadcastMapper.value.writeValueAsString(payload)
					)
				})
				.toDS()
			numEvents += seedEvents.count()

			// store seed events in test keyspace
			seedEvents.write
				.format("org.apache.spark.sql.cassandra")
				.options(mockEventsKeyspaceTableMap)
			  .mode(SaveMode.Append)
				.save()
		})
		log.info(s"Generated and persisted [$numEvents] seed events for [$numUniqueUsers] unique user.")

		// seed event record count
		val seedEventCount = sqlContext
			.read
			.format("org.apache.spark.sql.cassandra")
			.options(mockEventsKeyspaceTableMap)
			.load()
			.as[RawIdentityEvent]
			.count()
		log.info(s"Seed event record count [$seedEventCount].")

		// load static identity core events from Cassandra
		val staticCoreEvents = sqlContext
			.read
			.format("org.apache.spark.sql.cassandra")
			.options(mockEventsKeyspaceTableMap)
			.load()
			.as[RawIdentityEvent]

		// map static events to UniqueFields RDD
		val staticUniqueIds = staticCoreEvents
			.map(UniqueFields.from)
			.persist(StorageLevel.MEMORY_AND_DISK_SER)
		  .rdd

		// dedup static upmIds for each partition and write them to S3 as text files
		UniqueUpmIdProcessor.process(staticUniqueIds)
			.map({ case(upmId, _) => s"$upmId" })
			.saveAsTextFile(s"s3://$processorS3Bucket/test/upmIds/")

		// read static dedup'ed ids back from S3 and verify count
		val staticS3UniqueIds = sc.textFile(s"s3://$processorS3Bucket/test/upmIds/")
			.map(upmId => (upmId, 1))
			.reduceByKey(_ + _)
		log.info(s"Static dedup'ed upmId count from S3 [${staticS3UniqueIds.count()}]")

		// cleanup test bucket
		clearS3Data(sc, processorS3Bucket)

		// start dynamic event insertion at given rate
		staticUniqueIds.foreachPartitionAsync(_ => {
			val m = new ObjectMapper()
			m.registerModule(DefaultScalaModule)
			val limiter = RateLimiter.create(eventsPerSecond)
			while (true) {
				limiter.acquire()
				val id = random.nextInt(numUniqueUsers) + numUniqueUsers // new user
				val payload = IdentityPayload[CoreFields](
					id = upmId(id),
					time = IdentityPayload.time(randomTime),
					set = CoreFields(
						Some(upmId(id))
					)
				)
				cc.withSessionDo(
					session => {
						session.execute(
							s"""
								|INSERT INTO $mockEventsKeyspace.$mockEventsTable
								|  ( id, time, payload )
								|  VALUES ( '${payload.id}', ${payload.time}, '${m.writeValueAsString(payload)}' );
							""".stripMargin
					)
				})
				log.info(s"Inserted event for user [${upmId(id)}]")
			}
		})

		// load dynamic identity core events from Cassandra
		val coreEvents = sqlContext
			.read
			.format("org.apache.spark.sql.cassandra")
			.options(mockEventsKeyspaceTableMap)
			.load()
			.as[RawIdentityEvent]

		// map dynamic events to UniqueFields RDD
		val uniqueIds = coreEvents
			.map(UniqueFields.from)
			.persist(StorageLevel.MEMORY_AND_DISK_SER)
			.rdd

		// dedup dynamic upmIds for each partition and write them to S3 as text files
		UniqueUpmIdProcessor.process(uniqueIds)
			.map({ case(upmId, _) => s"$upmId" })
			.saveAsTextFile(s"s3://$processorS3Bucket/test/upmIds/")

		// read dynamic dedup'ed ids back from S3 and verify count
		val s3UniqueIds = sc.textFile(s"s3://$processorS3Bucket/test/upmIds/")
			.map(upmId => (upmId, 1))
			.reduceByKey(_ + _)
		log.info(s"Dynamic dedup'ed upmId count from S3 [${s3UniqueIds.count()}]")

		// final dynamic event record count
		val finalEventCount = sqlContext
			.read
			.format("org.apache.spark.sql.cassandra")
			.options(mockEventsKeyspaceTableMap)
			.load()
			.as[RawIdentityEvent]
		  .count()
		log.info(s"Final event record count [$finalEventCount] with [${finalEventCount - seedEventCount}] records inserted during processing.")

		// compare static and dynamic extraction sets
		val upmIdDiff = staticS3UniqueIds.leftOuterJoin(s3UniqueIds)
			.collect({
			  case (umpId, (_, None)) => umpId
		  })
			.collect
		log.info(s"UpmIds found in static set but missing from dynamic set [${upmIdDiff.mkString(", ")}]")

		// cleanup
		cc.withSessionDo(truncateMockEventsTable)

		sc.stop()
	}

	private def upmId(i: Int) = s"mock:upmId:$i"
	private def randomTime: DateTime = DateTime.now().minusDays(random.nextInt(1000) + 1)
	private def createMockEventsKeyspace(session: Session, numNodes: Int): Unit = session.execute(
		// taken from idndx-perf using DESCRIBE KEYSPACE core;
		s"""
			 |CREATE KEYSPACE IF NOT EXISTS $mockEventsKeyspace WITH replication = {
			 |  'class': 'NetworkTopologyStrategy',
			 |  'us-west-2': '$numNodes'
			 |};
			""".stripMargin
	)
	private def createMockEventsTable(session: Session): Unit = session.execute(
		// taken from idndx-perf using DESCRIBE TABLE core.event;
		s"""
			 |CREATE TABLE IF NOT EXISTS $mockEventsKeyspace.$mockEventsTable (
			 |  id text,
			 |  time timeuuid,
			 |  payload text,
			 |  PRIMARY KEY ((id), time)
			 |) WITH COMPACT STORAGE AND
			 |  bloom_filter_fp_chance=0.010000 AND
			 |  caching='KEYS_ONLY' AND
			 |  comment='' AND
			 |  dclocal_read_repair_chance=0.100000 AND
			 |  gc_grace_seconds=864000 AND
			 |  index_interval=128 AND
			 |  read_repair_chance=0.000000 AND
			 |  replicate_on_write='true' AND
			 |  populate_io_cache_on_flush='false' AND
			 |  default_time_to_live=0 AND
			 |  speculative_retry='99.0PERCENTILE' AND
			 |  memtable_flush_period_in_ms=0 AND
			 |  compaction={'class': 'SizeTieredCompactionStrategy'} AND
			 |  compression={'sstable_compression': 'LZ4Compressor'};
			""".stripMargin
	)
	private def truncateMockEventsTable(session: Session): Unit = session.execute(
		s"TRUNCATE $mockEventsKeyspace.$mockEventsTable;"
	)
	private def clearS3Data(sc: SparkContext, processorBucket: String) = {
		FileSystem
			.get(
				new URI(s"s3://$processorBucket"),
				sc.hadoopConfiguration
			)
			.delete(
				new Path(s"s3://$processorBucket/test/upmIds/"),
				true
			)
	}

}

class CassandraDynamicExtractionAppParams(arguments: Seq[String]) extends ScallopConf(arguments) {
	val env: ScallopOption[String] = opt[String]()
	val host: ScallopOption[String] = opt[String]()
	val port: ScallopOption[Int] = opt[Int]()
	val numUniqueUsers: ScallopOption[Int] = trailArg[Int]()
	val eventsPerSecond: ScallopOption[Int] = trailArg[Int]()
	verify()
}

