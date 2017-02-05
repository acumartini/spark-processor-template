package spark.message

import java.util.UUID

import com.amazonaws.services.sqs.model.{SendMessageBatchRequest, SendMessageBatchRequestEntry}
import com.fasterxml.jackson.databind.ObjectMapper
import spark.model.UniqueFields

import scala.collection.JavaConversions._

/**
	*
	* @author Adam Martini
	*/
case class UpmIdBatch(
	upmIdBatch: Iterable[String]
) {
	def toSendMessageBatchRequestEntry(mapper: ObjectMapper) = new SendMessageBatchRequestEntry(
		UUID.randomUUID().toString,
		mapper.writeValueAsString(upmIdBatch)
	)
}
object UpmIdBatch {
	def from(uniqueFieldsBatch: Iterable[UniqueFields]) = UpmIdBatch(uniqueFieldsBatch.map(_.upmId))
	def toSendMessageBatchRequest(queueUrl: String, mapper: ObjectMapper, upmIdBatches: Iterable[UpmIdBatch]) =
		new SendMessageBatchRequest(
			queueUrl,
			upmIdBatches.map(_.toSendMessageBatchRequestEntry(mapper)).toList
		)
}
