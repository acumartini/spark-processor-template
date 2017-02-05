package spark.processor

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import org.apache.spark.SparkConf
import org.scalatest.concurrent.Eventually
import org.scalatest.time.{Second, Seconds, Span}
import spark.AbstractSparkTest
import spark.message.IdBatch
import spark.model.UniqueFields
import spark.sqs.EmbeddedSqs

import scala.collection.convert.DecorateAsScala

class IdSqsProcessorTest extends AbstractSparkTest
	with EmbeddedSqs
	with Eventually
	with DecorateAsScala {

	val appName = "IdSqsProcessorTest"

	override val conf: SparkConf = sparkConf
	implicit override val patienceConfig =
		PatienceConfig(timeout = scaled(Span(20, Seconds)), interval = scaled(Span(1, Second)))

	behavior of "IdSqsProcessor"

	val mapper = new ObjectMapper()
	mapper.registerModule(DefaultScalaModule)

	def id(i: Int = 0) = s"mock:id:$i"

	it should "write batched UniqueFields to sqs in batches" in {
		val numIds = 1000

		Given("UniqueFields RDD")
		val uniqueFieldsObjs = (0 until numIds).map(i => UniqueFields(id(i)))
		val uniqueFieldsRDD = sc.parallelize(uniqueFieldsObjs)

		When("UniqueFields are batched and sent to SQS")
		IdSqsProcessor.process(
			uniqueFieldsRDD,
			sqs,
			testQueueName
		)
		var idCount = 0
		var receivedUniqueFields = List.empty[UniqueFields]
		eventually {
			val msg = sqs.client.receiveMessage(testQueueUrl)
			msg.getMessages.asScala
				.map(m => IdBatch(mapper.readValue(m.getBody, classOf[List[String]])))
				.foreach(batch => {
					val uniqueFieldsList = batch.idBatch.map(UniqueFields(_)).toList
					idCount += uniqueFieldsList.length
					receivedUniqueFields ++= uniqueFieldsList
				})
			idCount shouldEqual numIds
		}

		Then("Received ids equivalent to UniqueFields sent to SQS")
		receivedUniqueFields.foreach(uniqueFields =>
			uniqueFieldsObjs.contains(uniqueFields) shouldEqual true
		)
	}

}