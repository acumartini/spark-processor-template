package spark.processor

import com.datastax.spark.connector.cql.CassandraConnector
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import org.apache.spark.rdd.RDD
import spark.model.Template.{ContactFields, CoreFields, Event, EventPayload}
import spark.model.UniqueFields

import scala.collection.JavaConverters._
import spark.util.Joda._

/**
	* Accepts a RDD of UniqueFields and populates all missing and relevant fields.
	*/
object UniqueFieldsPopulationProcessor {

	def extractEvents(
		cc: CassandraConnector,
		coreKeyspace: String,
		coreEventsTable: String,
		contactKeyspace: String,
		contactEventsTable: String,
		uniqueFieldsRdd: RDD[UniqueFields]
	): RDD[UniqueFieldsEvents] = {
		uniqueFieldsRdd.mapPartitions(iter => {
			val mapper = new ObjectMapper()
			mapper.registerModule(DefaultScalaModule)

			iter.map(uniqueFields => {
				var coreEvents = List.empty[Event[EventPayload[CoreFields]]]
				var contactEvents = List.empty[Event[EventPayload[ContactFields]]]

				if (uniqueFields.otherId.isEmpty || uniqueFields.username.isEmpty) {
					// process core events
					coreEvents = cc.withSessionDo(session => {
						session.execute(
							s"SELECT * FROM $coreKeyspace.$coreEventsTable WHERE id = '${uniqueFields.id}';"
						)
							.all
							.asScala
							.flatMap(rawEvent =>
								Option(rawEvent.getString("payload")).map(e =>
									Event.from(
										EventPayload.from[CoreFields](
											mapper.readTree(e),
											mapper
										)
									))
							)
							.filter(event =>
								event.payload.set.otherId.nonEmpty ||
									event.payload.set.username.nonEmpty ||
									event.payload.rem.exists(rem => rem == "otherId" || rem == "username")
							)
							.sortBy(_.datetime)
							.toList
					})
				}

				if (uniqueFields.verifiedPhone.isEmpty) {
					// process contact events
					contactEvents = cc.withSessionDo(session => {
						session.execute(
							s"SELECT * FROM $contactKeyspace.$contactEventsTable WHERE id = '${uniqueFields.id}';"
						)
							.all
							.asScala
							.flatMap(rawEvent =>
								Option(rawEvent.getString("payload")).map(e =>
									Event.from(
										EventPayload.from[ContactFields](
											mapper.readTree(e),
											mapper
										)
									))
							)
							.filter(event =>
								event.payload.set.phone.nonEmpty ||
									event.payload.rem.contains("phone")
							)
							.sortBy(_.datetime)
							.toList
					})
				}

				UniqueFieldsEvents(uniqueFields, coreEvents, contactEvents)
			})
		})
	}

	def process(
		data: RDD[UniqueFieldsEvents]
	): RDD[UniqueFields] = {
		data.map(uniqueFieldsEvents => {
			var otherId = uniqueFieldsEvents.uniqueFields.otherId
			var username = uniqueFieldsEvents.uniqueFields.username
			var verifiedPhone = uniqueFieldsEvents.uniqueFields.verifiedPhone

			uniqueFieldsEvents.coreEvents.foreach(event => {
				event.payload.rem.foreach(rem => {
					if (rem == "otherId") {
						otherId = None
					} else if (rem == "username") {
						username = None
					}
				})
				event.payload.set.otherId.foreach(newOtherId => otherId = Some(newOtherId))
				event.payload.set.username.foreach(newUsername => username = Some(newUsername))
			})

			uniqueFieldsEvents.contactEvents.foreach(event => {
				if (event.payload.rem.contains("phone")) {
					verifiedPhone = None
				}
				event.payload.set.phone.foreach(newVerifiedPhone => verifiedPhone = Some(newVerifiedPhone))
			})

			uniqueFieldsEvents.uniqueFields.copy(
				otherId = otherId,
				username = username,
				verifiedPhone = verifiedPhone
			)
		})
	}

	def extractAndProcess(
		cc: CassandraConnector,
		coreKeyspace: String,
		coreEventsTable: String,
		contactKeyspace: String,
		contactEventsTable: String,
		uniqueFieldsRdd: RDD[UniqueFields]
	): RDD[UniqueFields] = {
		process(extractEvents(
			cc,
			coreKeyspace,
			coreEventsTable,
			contactKeyspace,
			contactEventsTable,
			uniqueFieldsRdd
		))
	}

}

case class UniqueFieldsEvents(
	uniqueFields: UniqueFields,
	coreEvents: List[Event[EventPayload[CoreFields]]] = List(),
	contactEvents: List[Event[EventPayload[ContactFields]]] = List()
)
