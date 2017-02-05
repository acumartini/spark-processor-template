package spark.client

import com.amazonaws.services.sqs.AmazonSQSClient
import com.amazonaws.services.sqs.model.{CreateQueueRequest, GetQueueUrlRequest}
import spark.serialization.SerializableAWSCredentials

/**
	*
	* @author Adam Martini
	*/
class SQSClient(endpoint: String, creds: Option[SerializableAWSCredentials] = None) extends Serializable {

	@transient private var sqs: AmazonSQSClient = _

	def client: AmazonSQSClient = {
		if (sqs == null) {
			if (creds.nonEmpty) {
				sqs = new AmazonSQSClient(creds.get)
			} else {
				sqs = new AmazonSQSClient()
			}
			sqs.setEndpoint(endpoint)
		}
		sqs
	}

	def createQueue(queueName: String): String = {
		val createQueueRequest = new CreateQueueRequest(queueName)
		val queueUrl = client.createQueue(createQueueRequest).getQueueUrl
		queueUrl
	}

	def getQueueUrl(queueName: String): String = {
		val getQueueUrlRequest = new GetQueueUrlRequest(queueName)
		client.getQueueUrl(getQueueUrlRequest).getQueueUrl
	}

}
