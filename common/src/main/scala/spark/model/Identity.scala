package spark.model

import com.fasterxml.jackson.annotation.{JsonIgnoreProperties, JsonProperty}
import com.fasterxml.jackson.databind.{JsonNode, ObjectMapper}
import spark.model.Identity.{IdentityFields, IdentityPayload}
import org.apache.cassandra.utils.UUIDGen
import org.joda.time.DateTime

import scala.annotation.meta.field
import scala.reflect.ClassTag

/**
	*
	* @author Adam Martini
	*/
object Identity {

	case class IdentityOmega(
		user_upm_id: String,
		allowTagging: Option[Boolean],
		health_basicacceptance: Option[String],
		health_enhancedacceptance: Option[String],
		record_created_date: Option[String]
	)


	trait IdentityFields
	@JsonIgnoreProperties(ignoreUnknown = true)
	case class CoreFields(
		upmId: Option[String] = None,
		nuId: Option[String] = None,
		username: Option[String] = None
	) extends IdentityFields
	@JsonIgnoreProperties(ignoreUnknown = true)
	case class ContactFields(
		verifiedphone: Option[String] = None
	) extends IdentityFields

	case class IdentityPayload[T <: IdentityFields](
		@(JsonProperty @field)("$rem") rem: List[String] = List.empty,
		model: String = "",
		id: String,
		time: Long = IdentityPayload.time(DateTime.now()),
		source: String = "",
		@(JsonProperty @field)("$set") set: T,
		version: Int = 1,
		snapshot: Boolean = false
	)
	object IdentityPayload {
		def time(datetime: DateTime): Long = datetime.getMillis
		def from[T <: IdentityFields](payload: JsonNode, mapper: ObjectMapper)(implicit classTag: ClassTag[T]): IdentityPayload[T] = IdentityPayload[T](
			rem = mapper.readValue(payload.get("$rem").toString, classOf[List[String]]),
			model = payload.get("model").asText,
			id = payload.get("id").asText,
			time = payload.get("time").asLong,
			source = payload.get("source").asText,
			set = mapper.readValue(payload.get("$set").toString, classTag.runtimeClass.asInstanceOf[Class[T]]),
			version = payload.get("version").asInt,
			snapshot = payload.get("snapshot").asBoolean
		)
	}

	case class IdentityEvent[T <: IdentityPayload[_ <: IdentityFields]](
		upmId: String,
		datetime: DateTime,
		payload: T
	)
	object IdentityEvent {
		def from[T <: IdentityPayload[_ <: IdentityFields]](payload: T): IdentityEvent[T] = IdentityEvent[T](
			payload.id,
			new DateTime(payload.time),
			payload
		)
	}

	trait UniquenessIndexKeyValue {
		def key: String
		def value: String
	}
	object UniquenessIndexKeyValue {
		val nuIdLookupKey = "__U:core:1:nuId"
		val usernameLookupKey = "__U:core:1:username"
		val verifiedPhoneLookupKey = "__U:contact:1:verifiedphone"

		def keyFor(prefix: String, value: String) = s"$prefix:$value"
		def valueFrom(prefix: String): String = prefix.substring(prefix.lastIndexOf(":") + 1)
	}
	case class UniquenessIndex(
		key: String,
		value: String
	) extends UniquenessIndexKeyValue
	case class UniquenessIndexLookup(
		id: String,
		key: String,
		value: String
	) extends UniquenessIndexKeyValue

}

// Note: Moved the following case classes out of their enclosing Identity parent object due to the limitations of
// implicit DataSet encoders in spark-sql_1.6.1. Parent classes are handled well in spark-sql_2.x.

case class RawIdentityEvent(
	id: String,
	time: String,
	payload: String
)
object RawIdentityEvent {
	def from[T <: IdentityPayload[_ <: IdentityFields]](payload: T, payloadStr: String) = RawIdentityEvent(
		payload.id,
		UUIDGen.getTimeUUID(payload.time).toString,
		payloadStr
	)
}
