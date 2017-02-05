package spark.app

import com.typesafe.config.{Config, ConfigFactory}
import org.apache.spark.sql.SparkSession
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkConf, SparkContext}
import org.rogach.scallop.{ScallopConf, ScallopOption}
import spark.cassandra.{Connector, IdentityDB}
import spark.client.SNSClient
import spark.model.{RawIdentityEvent, UniqueFields}
import spark.processor.{UniqueUpmIdProcessor, UpmIdSnsProcessor}

/**
	*
	* @author Adam Martini
	*/
object UpmIdSnsPOC {

	def main(args: Array[String]): Unit = {
		// parse optional arguments
		val params = new UpmIdSnsParams(args)
		val config: Config = ConfigFactory.load(s"processor-${params.env.getOrElse("test")}")
		val snsRegion = config.getString("corroborator.sns.region")
		val snsTopicArn = config.getString("corroborator.sns.topicArn")


		// setup spark context
		val conf: SparkConf = new SparkConf()
			.setAppName("CassandraConnectorPOC")
			.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
			.registerKryoClasses(Array(classOf[RawIdentityEvent], classOf[UniqueFields]))
			.set("spark.cassandra.connection.host", params.host.getOrElse(Connector.host))
			.set("spark.cassandra.connection.port", params.port.getOrElse(Connector.port).toString)
			.set("spark.default.parallelism", "100")
		val sc = new SparkContext(conf)

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
		val uniqueFields = coreEvents
			.map(UniqueFields.from)
			.persist(StorageLevel.MEMORY_AND_DISK_SER)
		  .rdd

		// dedup user events
		val dedupedUniqueFields = UniqueUpmIdProcessor.process(uniqueFields)
			.map({ case(upmId,  _) => UniqueFields(upmId) })

		// write upmIds to SNS
		UpmIdSnsProcessor.process(
			dedupedUniqueFields,
			new SNSClient(snsRegion),
			snsTopicArn
		)

		sc.stop()
	}
}

class UpmIdSnsParams(arguments: Seq[String]) extends ScallopConf(arguments) {
	val host: ScallopOption[String] = opt[String]()
	val port: ScallopOption[Int] = opt[Int]()
	val env: ScallopOption[String] = opt[String]()
	verify()
}

