package spark.app

import com.datastax.spark.connector.cql.CassandraConnector
import com.typesafe.config.{Config, ConfigFactory}
import org.apache.spark.sql.SparkSession
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkConf, SparkContext}
import org.rogach.scallop.{ScallopConf, ScallopOption}
import spark.cassandra.IdentityDB._
import spark.cassandra.{Connector, IdentityDB}
import spark.model.{RawIdentityEvent, UniqueFields}
import spark.processor.{UniqueFieldsPopulateWriteProcessor, UniqueUpmIdProcessor}

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
			.registerKryoClasses(Array(classOf[RawIdentityEvent], classOf[UniqueFields]))
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

		// load identity core events from Cassandra
		val coreEvents = sqlContext
			.read
			.format("org.apache.spark.sql.cassandra")
			.options(IdentityDB.CoreTable.keyspaceTableMap)
			.load()
			.as[RawIdentityEvent]

		// map events to uniqueIds RDD
		val uniqueIds = coreEvents
			.map(UniqueFields.from)
			.persist(StorageLevel.MEMORY_AND_DISK_SER)
		  .rdd

		// dedup upmIds for each partition and write them to S3 as text files
		val unpopulatedUniqueFieldsRDD = UniqueUpmIdProcessor.process(uniqueIds)
			.map({ case(upmId, _) => UniqueFields(upmId) })

		// populate and write unique fields data
		UniqueFieldsPopulateWriteProcessor.process(
			unpopulatedUniqueFieldsRDD,
			cc,
			IdentityDB.CoreTable.keyspace,
			IdentityDB.CoreTable.table,
			IdentityDB.ContactTable.keyspace,
			IdentityDB.ContactTable.table,
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

