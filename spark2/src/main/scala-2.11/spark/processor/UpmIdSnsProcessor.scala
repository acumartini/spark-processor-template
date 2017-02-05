package spark.processor

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import org.apache.spark.rdd.RDD
import spark.client.SNSClient
import spark.message.UpmIdBatch
import spark.model.UniqueFields

/**
	* Accepts a RDD of UniqueFields, batches them by the given batch size, and publishes them to the given SNS topic.
	*/
object UpmIdSnsProcessor {

	def process(
		data: RDD[UniqueFields],
		sns: SNSClient,
		topicArn: String,
		batchSize: Int = 50
	): Unit = {
		data
			.foreachPartition(iter => {
				sns.client // instantiate the @transient client at partition level

				val mapper = new ObjectMapper()
				mapper.registerModule(DefaultScalaModule)

				iter
					.grouped(batchSize)
					.map(UpmIdBatch.from)
				  .foreach(batch => sns.client.publish(
					  topicArn,
					  mapper.writeValueAsString(batch.upmIdBatch))
				  )
			})
	}

}
