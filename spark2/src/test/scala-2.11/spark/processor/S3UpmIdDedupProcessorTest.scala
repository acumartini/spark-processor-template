package spark.processor

import org.apache.spark.SparkConf
import spark.model.UniqueFields
import spark.{AbstractSparkTest, SparkS3TestBucket}


class S3UpmIdDedupProcessorTest extends AbstractSparkTest
	with SparkS3TestBucket {

	val appName = "S3UpmIdDedupProcessorTest"

	override val conf: SparkConf = sparkConf

	behavior of "S3UpmIdDedupProcessor"

	it should "extract a set of unique upmIds from a collection of UniqueFields" in {
		Given("UpmIds from any some source")
		val uniqueIds = sc.parallelize(Seq(
			UniqueFields("1"),
			UniqueFields("2"),
			UniqueFields("2"),
			UniqueFields("3"),
			UniqueFields("3"),
			UniqueFields("3")
		))

		When("UniqueFields persisted to S3")
		val path = testDataPath // to prevent serialization errors caused by anonymous function (uniqueKeyFunc) below
		S3UpmIdDedupProcessor.process[UniqueFields](
			uniqueIds,
			bucket,
			(uniqueIds: UniqueFields) => s"$path/${uniqueIds.upmId}",
			s3 = s3
		)

		Then("UniqueFields are dedup'ed by unique s3 key")
		val summaries = s3.client.listObjects(bucket, testDataPath).getObjectSummaries.asScala
		summaries.length shouldEqual 3
		summaries.find(_.getKey == s"$bucket/$testDataPath/1")
		summaries.find(_.getKey == s"$bucket/$testDataPath/2")
		summaries.find(_.getKey == s"$bucket/$testDataPath/3")
	}

}