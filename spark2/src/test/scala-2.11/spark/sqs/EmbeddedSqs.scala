package spark.sqs

import spark.UnitSpec
import spark.serialization.SerializableAWSCredentials
import org.elasticmq.rest.sqs.{SQSRestServer, SQSRestServerBuilder}
import org.scalatest.BeforeAndAfterAll
import spark.client.SQSClient


/**
	*
	* @author Adam Martini
	*/
trait EmbeddedSqs extends UnitSpec
	with BeforeAndAfterAll {

	private val port = 9325
	private val host = "localhost"

	val sqsQueueEndpoint = s"http://$host:$port"
	val sqsServer: SQSRestServer = SQSRestServerBuilder.withPort(port).withInterface(host).start()
	val testQueueName = "testQueue"

	lazy val sqs = new SQSClient(sqsQueueEndpoint, Some(SerializableAWSCredentials("x", "x")))
	lazy val testQueueUrl: String = sqs.getQueueUrl(testQueueName)

	override def beforeAll(): Unit = {
		super.beforeAll()
		sqs.createQueue(testQueueName)
	}

	override def afterAll(): Unit = {
		sqsServer.stopAndWait()
		super.afterAll()
	}

}