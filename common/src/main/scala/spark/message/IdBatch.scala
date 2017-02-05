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
case class IdBatch(
	idBatch: Iterable[String]
) {
	def toSendMessageBatchRequestEntry(mapper: ObjectMapper) = new SendMessageBatchRequestEntry(
		UUID.randomUUID().toString,
		mapper.writeValueAsString(idBatch)
	)
}
object IdBatch {
	def from(uniqueFieldsBatch: Iterable[UniqueFields]) = IdBatch(uniqueFieldsBatch.map(_.id))
	def toSendMessageBatchRequest(queueUrl: String, mapper: ObjectMapper, idBatches: Iterable[IdBatch]) =
		new SendMessageBatchRequest(
			queueUrl,
			idBatches.map(_.toSendMessageBatchRequestEntry(mapper)).toList
		)
}
