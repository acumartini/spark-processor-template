package spark.processor

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import org.apache.spark.SparkConf
import org.scalatest.concurrent.Eventually
import org.scalatest.time.{Second, Seconds, Span}
import spark.AbstractSparkTest
import spark.message.UpmIdBatch
import spark.model.UniqueFields
import spark.sqs.EmbeddedSqs

import scala.collection.convert.DecorateAsScala

class UpmIdSqsProcessorTest extends AbstractSparkTest
	with EmbeddedSqs
	with Eventually
	with DecorateAsScala {

	val appName = "UpmIdSqsProcessorTest"

	override val conf: SparkConf = sparkConf
	implicit override val patienceConfig =
		PatienceConfig(timeout = scaled(Span(20, Seconds)), interval = scaled(Span(1, Second)))

	behavior of "UpmIdSqsProcessor"

	val mapper = new ObjectMapper()
	mapper.registerModule(DefaultScalaModule)

	def upmId(i: Int = 0) = s"mock:upmId:$i"

	it should "write batched UniqueFields to sqs in batches" in {
		val numIds = 1000

		Given("UniqueFields RDD")
		val uniqueFieldsObjs = (0 until numIds).map(i => UniqueFields(upmId(i)))
		val uniqueFieldsRDD = sc.parallelize(uniqueFieldsObjs)

		When("UniqueFields are batched and sent to SQS")
		UpmIdSqsProcessor.process(
			uniqueFieldsRDD,
			sqs,
			testQueueName
		)
		var upmIdCount = 0
		var receivedUniqueFields = List.empty[UniqueFields]
		eventually {
			val msg = sqs.client.receiveMessage(testQueueUrl)
			msg.getMessages.asScala
				.map(m => UpmIdBatch(mapper.readValue(m.getBody, classOf[List[String]])))
				.foreach(batch => {
					val uniqueFieldsList = batch.upmIdBatch.map(UniqueFields(_)).toList
					upmIdCount += uniqueFieldsList.length
					receivedUniqueFields ++= uniqueFieldsList
				})
			upmIdCount shouldEqual numIds
		}

		Then("Received upmIds equivalent to UniqueFields sent to SQS")
		receivedUniqueFields.foreach(uniqueFields =>
			uniqueFieldsObjs.contains(uniqueFields) shouldEqual true
		)
	}

}