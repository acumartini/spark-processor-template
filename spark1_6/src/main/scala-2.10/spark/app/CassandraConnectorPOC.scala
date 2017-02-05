package spark.app

import com.typesafe.config.{Config, ConfigFactory}
import org.apache.spark.sql.SQLContext
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkConf, SparkContext}
import org.rogach.scallop.{ScallopConf, ScallopOption}
import spark.cassandra.{Connector, IdentityDB}
import spark.client.S3Client
import spark.model.{RawIdentityEvent, UniqueFields}
import spark.processor.S3UpmIdDedupProcessor
import spark.serialization.SerializableAWSCredentials

/**
	*
	* @author Adam Martini
	*/
object CassandraConnectorPOC {
	private val config: Config = ConfigFactory.load("processor-test") // TODO: dynamic env via argument parsing
	private val processorAccessKey = config.getString("spark.s3.processor.access.key")
	private val processorSecretKey = config.getString("spark.s3.processor.secret.key")
	private val processorS3Bucket = config.getString("processor.s3.bucket")

	def main(args: Array[String]): Unit = {
		// parse optional arguments
		val params = new AppParams(args)

		// setup spark context
		val conf: SparkConf = new SparkConf()
			.setAppName("CassandraConnectorPOC")
			.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
			.registerKryoClasses(Array(classOf[RawIdentityEvent], classOf[UniqueFields]))
			.set("spark.cassandra.connection.host", params.host.getOrElse(Connector.host))
			.set("spark.cassandra.connection.port", params.port.getOrElse(Connector.port).toString)
		val sc = new SparkContext(conf)

		// setup sql context
		val sqlContext = new SQLContext(sc)
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

		// dedup upmIds using S3
		S3UpmIdDedupProcessor.process[UniqueFields](
			uniqueIds,
			processorS3Bucket,
			(uniqueIds: UniqueFields) => s"output/upmId/${uniqueIds.upmId}",
			s3 = new S3Client(Some(SerializableAWSCredentials(
				accessKey = processorAccessKey,
				secretKey = processorSecretKey
			)))
		)

		sc.stop()
	}
}

class AppParams(arguments: Seq[String]) extends ScallopConf(arguments) {
	val host: ScallopOption[String] = opt[String]()
	val port: ScallopOption[Int] = opt[Int]()
	verify()
}

