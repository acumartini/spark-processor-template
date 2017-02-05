package spark.app

import com.typesafe.config.{Config, ConfigFactory}
import org.apache.spark.sql.SparkSession
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkConf, SparkContext}
import spark.model.Identity.IdentityOmega
import spark.model.UniqueFields
import spark.processor.S3UpmIdDedupProcessor

/**
	*
	* @author Adam Martini
	*/
object UniqueUpmIdPOC {
	private val config: Config = ConfigFactory.load("processor-test")
	private val identityDataPathPrefix = config.getString("identity.s3.data.path")
	private val processorS3Bucket = config.getString("processor.s3.bucket")

	def main(args: Array[String]): Unit = {
		// setup spark context
		val conf: SparkConf = new SparkConf()
			.setAppName("UniqueUpmIdPOC")
			.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
			.registerKryoClasses(Array(classOf[IdentityOmega], classOf[UniqueFields]))
		val sc = new SparkContext(conf)

		// setup sql context
		val sqlContext = SparkSession
			.builder()
			.getOrCreate()
		import sqlContext.implicits._ // required for implicit Encoders

		// load identity UniqueFields from S3
		val identityDataPath = s"s3://$identityDataPathPrefix/identity_omega/*"
		val identityData = sqlContext.read.option("mergeSchema", "false").parquet(identityDataPath)
			.as[IdentityOmega]
			.map(UniqueFields.from)
			.persist(StorageLevel.MEMORY_AND_DISK_SER)
			.rdd

		// dedup upmIds using S3
		S3UpmIdDedupProcessor.process[UniqueFields](
			identityData,
			processorS3Bucket,
			(uniqueIds: UniqueFields) => s"output/upmId/${uniqueIds.upmId}"
		)

		sc.stop()
	}
}
