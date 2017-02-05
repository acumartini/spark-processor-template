package spark.processor

import com.datastax.spark.connector.cql.CassandraConnector
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import org.apache.spark.rdd.RDD
import spark.model.Identity.{ContactFields, CoreFields, IdentityEvent, IdentityPayload}
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
				var coreEvents = List.empty[IdentityEvent[IdentityPayload[CoreFields]]]
				var contactEvents = List.empty[IdentityEvent[IdentityPayload[ContactFields]]]

				if (uniqueFields.nuId.isEmpty || uniqueFields.username.isEmpty) {
					// process core events
					coreEvents = cc.withSessionDo(session => {
						session.execute(
							s"SELECT * FROM $coreKeyspace.$coreEventsTable WHERE id = '${uniqueFields.upmId}';"
						)
							.all
							.asScala
							.flatMap(rawEvent =>
								Option(rawEvent.getString("payload")).map(e =>
									IdentityEvent.from(
										IdentityPayload.from[CoreFields](
											mapper.readTree(e),
											mapper
										)
									))
							)
							.filter(event =>
								event.payload.set.nuId.nonEmpty ||
									event.payload.set.username.nonEmpty ||
									event.payload.rem.exists(rem => rem == "nuId" || rem == "username")
							)
							.sortBy(_.datetime)
							.toList
					})
				}

				if (uniqueFields.verifiedPhone.isEmpty) {
					// process contact events
					contactEvents = cc.withSessionDo(session => {
						session.execute(
							s"SELECT * FROM $contactKeyspace.$contactEventsTable WHERE id = '${uniqueFields.upmId}';"
						)
							.all
							.asScala
							.flatMap(rawEvent =>
								Option(rawEvent.getString("payload")).map(e =>
									IdentityEvent.from(
										IdentityPayload.from[ContactFields](
											mapper.readTree(e),
											mapper
										)
									))
							)
							.filter(event =>
								event.payload.set.verifiedphone.nonEmpty ||
									event.payload.rem.contains("verifiedphone")
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
			var nuId = uniqueFieldsEvents.uniqueFields.nuId
			var username = uniqueFieldsEvents.uniqueFields.username
			var verifiedPhone = uniqueFieldsEvents.uniqueFields.verifiedPhone

			uniqueFieldsEvents.coreEvents.foreach(event => {
				event.payload.rem.foreach(rem => {
					if (rem == "nuId") {
						nuId = None
					} else if (rem == "username") {
						username = None
					}
				})
				event.payload.set.nuId.foreach(newNuId => nuId = Some(newNuId))
				event.payload.set.username.foreach(newUsername => username = Some(newUsername))
			})

			uniqueFieldsEvents.contactEvents.foreach(event => {
				if (event.payload.rem.contains("verifiedphone")) {
					verifiedPhone = None
				}
				event.payload.set.verifiedphone.foreach(newVerifiedPhone => verifiedPhone = Some(newVerifiedPhone))
			})

			uniqueFieldsEvents.uniqueFields.copy(
				nuId = nuId,
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
	coreEvents: List[IdentityEvent[IdentityPayload[CoreFields]]] = List(),
	contactEvents: List[IdentityEvent[IdentityPayload[ContactFields]]] = List()
)
