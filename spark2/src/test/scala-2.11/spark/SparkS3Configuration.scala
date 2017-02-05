package spark

import java.util.UUID

import spark.client.S3Client
import spark.serialization.SerializableAWSCredentials

import scala.collection.convert.DecorateAsScala


/**
	*
	* @author Adam Martini
	*/
trait SparkS3Configuration extends AbstractSparkTest {

	val accessKey: String = config.getString("spark.s3.processor.access.key")
	val secretKey: String = config.getString("spark.s3.processor.secret.key")
	val bucket: String = "idn-processor"

	override def beforeAll() {
		super.beforeAll()

		// set hadoop s3 credentials for UserServices spark-s3-processor user
		sc.hadoopConfiguration.set("fs.s3a.access.key", accessKey)
		sc.hadoopConfiguration.set("fs.s3a.secret.key", secretKey)
	}

}

trait SparkS3TestBucket extends SparkS3Configuration with DecorateAsScala {

	@transient val s3 = new S3Client(Some(SerializableAWSCredentials(accessKey, secretKey)))
	val testDataPath = s"$bucket/test/${UUID.randomUUID()}"

	override def afterAll() {
		s3.client.listObjects(bucket, testDataPath)
			.getObjectSummaries.asScala
			.foreach(summary => s3.client.deleteObject(bucket, summary.getKey))
		super.afterAll()
	}

}
