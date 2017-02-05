package spark.model

import com.fasterxml.jackson.annotation.{JsonIgnoreProperties, JsonProperty}
import com.fasterxml.jackson.databind.{JsonNode, ObjectMapper}
import spark.model.Template.{EventFields, EventPayload}
import org.apache.cassandra.utils.UUIDGen
import org.joda.time.DateTime

import scala.annotation.meta.field
import scala.reflect.ClassTag

/**
	*
	* @author Adam Martini
	*/
object Template {

	trait EventFields
	@JsonIgnoreProperties(ignoreUnknown = true)
	case class CoreFields(
		id: Option[String] = None,
		otherId: Option[String] = None,
		username: Option[String] = None
	) extends EventFields
	@JsonIgnoreProperties(ignoreUnknown = true)
	case class ContactFields(
		phone: Option[String] = None
	) extends EventFields

	case class EventPayload[T <: EventFields](
		@(JsonProperty @field)("$rem") rem: List[String] = List.empty,
		model: String = "",
		id: String,
		time: Long = EventPayload.time(DateTime.now()),
		source: String = "",
		@(JsonProperty @field)("$set") set: T,
		version: Int = 1,
		snapshot: Boolean = false
	)
	object EventPayload {
		def time(datetime: DateTime): Long = datetime.getMillis
		def from[T <: EventFields](payload: JsonNode, mapper: ObjectMapper)(implicit classTag: ClassTag[T]): EventPayload[T] = EventPayload[T](
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

	case class Event[T <: EventPayload[_ <: EventFields]](
		id: String,
		datetime: DateTime,
		payload: T
	)
	object Event {
		def from[T <: EventPayload[_ <: EventFields]](payload: T): Event[T] = Event[T](
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
		val otherIdLookupKey = "__U:core:1:otherId"
		val usernameLookupKey = "__U:core:1:username"
		val verifiedPhoneLookupKey = "__U:contact:1:phone"

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

// Note: Moved the following case classes out of their enclosing Template parent object due to the limitations of
// implicit DataSet encoders in spark-sql_1.6.1. Parent classes are handled well in spark-sql_2.x.

case class RawEvent(
	id: String,
	time: String,
	payload: String
)
object RawEvent {
	def from[T <: EventPayload[_ <: EventFields]](payload: T, payloadStr: String) = RawEvent(
		payload.id,
		UUIDGen.getTimeUUID(payload.time).toString,
		payloadStr
	)
}
