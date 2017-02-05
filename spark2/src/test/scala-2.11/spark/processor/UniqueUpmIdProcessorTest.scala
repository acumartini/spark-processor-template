package spark.processor

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import spark.model.Identity.IdentityOmega
import spark.model.UniqueFields
import spark.{AbstractSparkTest, SparkS3Configuration, SparkSqlContext}

class UniqueUpmIdProcessorTest extends AbstractSparkTest
	with SparkS3Configuration
	with SparkSqlContext {

	private val identityDataPathPrefix = config.getString("identity.s3.data.path")

	val appName = "UniqueUpmIdProcessorTest"

	override val conf: SparkConf = sparkConf
		.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
		.registerKryoClasses(Array(classOf[IdentityOmega], classOf[UniqueFields]))

	behavior of "UniqueUpmIdProcessor"

	it should "extract a set of unique upmIds from a collection of UniqueFields" in {
		val sqlContext = sqx
		import sqlContext.implicits._ // required for implicit Encoders

		Given("Identity data from S3")
		// load identity UniqueFields from S3
		val identityDataPath = s"s3a://$identityDataPathPrefix/identity.parquet"
		val identityData = sqx.read.option("mergeSchema", "false").parquet(identityDataPath)
			.as[IdentityOmega]
		  .map(UniqueFields.from)
			.persist(StorageLevel.MEMORY_AND_DISK_SER)
			.rdd

		When("UniqueFields are reduced")
		val resRdd: RDD[(String, Int)] = UniqueUpmIdProcessor.process(identityData)
		val result = resRdd.collect()

		Then("Produces the correct number of upmIds")
		result.length shouldEqual 855598
	}

}