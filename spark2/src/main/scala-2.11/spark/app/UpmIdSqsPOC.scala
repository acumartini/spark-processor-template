package spark.app

import com.typesafe.config.{Config, ConfigFactory}
import org.apache.spark.sql.SparkSession
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkConf, SparkContext}
import org.rogach.scallop.{ScallopConf, ScallopOption}
import spark.cassandra.{Connector, TemplateDB}
import spark.client.SQSClient
import spark.model.{RawEvent, UniqueFields}
import spark.processor.{UniqueIdProcessor, IdSqsProcessor}

/**
	*
	* @author Adam Martini
	*/
object IdSqsPOC {

	def main(args: Array[String]): Unit = {
		// parse optional arguments
		val params = new IdSqsParams(args)
		val config: Config = ConfigFactory.load(s"processor-${params.env.getOrElse("test")}")
		val sqsEndpoint = config.getString("corroborator.sqs.endpoint")
		val sqsQueueName = config.getString("corroborator.sqs.queueName ")

		// setup spark context
		val conf: SparkConf = new SparkConf()
			.setAppName("CassandraConnectorPOC")
			.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
			.registerKryoClasses(Array(classOf[RawEvent], classOf[UniqueFields]))
			.set("spark.cassandra.connection.host", params.host.getOrElse(Connector.host))
			.set("spark.cassandra.connection.port", params.port.getOrElse(Connector.port).toString)
		val sc = new SparkContext(conf)

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
		val uniqueFields = coreEvents
			.map(UniqueFields.from)
			.persist(StorageLevel.MEMORY_AND_DISK_SER)
		  .rdd

		// dedup user events
		val dedupedUniqueFields = UniqueIdProcessor.process(uniqueFields)
			.map({ case(id,  _) => UniqueFields(id) })

		// write ids to SQS
		IdSqsProcessor.process(
			dedupedUniqueFields,
			new SQSClient(sqsEndpoint),
			sqsQueueName
		)

		sc.stop()
	}
}

class IdSqsParams(arguments: Seq[String]) extends ScallopConf(arguments) {
	val host: ScallopOption[String] = opt[String]()
	val port: ScallopOption[Int] = opt[Int]()
	val env: ScallopOption[String] = opt[String]()
	verify()
}

