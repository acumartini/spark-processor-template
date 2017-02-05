package spark.app

import com.datastax.spark.connector.cql.CassandraConnector
import com.typesafe.config.{Config, ConfigFactory}
import org.apache.spark.sql.SparkSession
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkConf, SparkContext}
import org.rogach.scallop.{ScallopConf, ScallopOption}
import spark.cassandra.TemplateDB._
import spark.cassandra.{Connector, TemplateDB}
import spark.model.{RawEvent, UniqueFields}
import spark.processor.{UniqueFieldsPopulateWriteProcessor, UniqueIdProcessor}

/**
	*
	* @author Adam Martini
	*/
object UniquenessIndexPopulateWritePOC {

	def main(args: Array[String]): Unit = {
		// parse optional arguments
		val params = new UniquenessIndexPopulateWriteParams(args)
		val config: Config = ConfigFactory.load(s"processor-${params.env.getOrElse("test")}")

		// setup spark context
		val conf: SparkConf = new SparkConf()
			.setAppName("CassandraConnectorPOC")
			.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
			.registerKryoClasses(Array(classOf[RawEvent], classOf[UniqueFields]))
			.set("spark.cassandra.connection.host", params.host.getOrElse(Connector.host))
			.set("spark.cassandra.connection.port", params.port.getOrElse(Connector.port).toString)
			.set("spark.default.parallelism", "100")
		val sc = new SparkContext(conf)
		val cc = CassandraConnector(sc.getConf)

		// setup sql context
		val sqlContext = SparkSession
			.builder()
			.getOrCreate()
		import sqlContext.implicits._ // required for implicit Encoders

		// load template core events from Cassandra
		val coreEvents = sqlContext
			.read
			.format("org.apache.spark.sql.cassandra")
			.options(TemplateDB.CoreTable.keyspaceTableMap)
			.load()
			.as[RawEvent]

		// map events to uniqueIds RDD
		val uniqueIds = coreEvents
			.map(UniqueFields.from)
			.persist(StorageLevel.MEMORY_AND_DISK_SER)
		  .rdd

		// dedup ids for each partition and write them to S3 as text files
		val unpopulatedUniqueFieldsRDD = UniqueIdProcessor.process(uniqueIds)
			.map({ case(id, _) => UniqueFields(id) })

		// populate and write unique fields data
		UniqueFieldsPopulateWriteProcessor.process(
			unpopulatedUniqueFieldsRDD,
			cc,
			TemplateDB.CoreTable.keyspace,
			TemplateDB.CoreTable.table,
			TemplateDB.ContactTable.keyspace,
			TemplateDB.ContactTable.table,
			UniquenessKeyspace.keyspace,
			UniquenessIndexTable.table,
			UniquenessLookupIndexTable.table
		)

		sc.stop()
	}

}

class UniquenessIndexPopulateWriteParams(arguments: Seq[String]) extends ScallopConf(arguments) {
	val host: ScallopOption[String] = opt[String]()
	val port: ScallopOption[Int] = opt[Int]()
	val env: ScallopOption[String] = opt[String]()
	verify()
}

