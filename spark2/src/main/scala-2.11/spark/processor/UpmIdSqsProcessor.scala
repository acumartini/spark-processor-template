package spark.processor

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import org.apache.spark.rdd.RDD
import spark.client.SQSClient
import spark.message.UpmIdBatch
import spark.model.UniqueFields

/**
	* Accepts a RDD of UniqueFields, batches them by the given batch size, and sends them as batch messages to the given
	* SQS queue.
	*/
object UpmIdSqsProcessor {

	def process(
		data: RDD[UniqueFields],
		sqs: SQSClient,
		queueName: String,
		batchSize: Int = 50
	): Unit = {
		data
			.foreachPartition(iter => {
				val queueUrl =  sqs.getQueueUrl(queueName)

				val mapper = new ObjectMapper()
				mapper.registerModule(DefaultScalaModule)

				iter
					.grouped(batchSize)
					.map(UpmIdBatch.from)
					.grouped(10)
					.foreach(batch => sqs.client.sendMessageBatch(
						UpmIdBatch.toSendMessageBatchRequest(queueUrl, mapper, batch)
					))
			})
	}

}
