package spark.processor

import com.datastax.spark.connector.cql.CassandraConnector
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import org.apache.spark.rdd.RDD
import spark.model.Template._
import spark.model.UniqueFields

import scala.collection.JavaConverters._
import spark.util.Joda._


/**
	* Accepts a RDD of UniqueFields and populates all missing and relevant fields.
	*/
object UniqueFieldsPopulateWriteProcessor {

	def process(
		uniqueFieldsRdd: RDD[UniqueFields],
		cc: CassandraConnector,
		coreKeyspace: String,
		coreEventsTable: String,
		contactKeyspace: String,
		contactEventsTable: String,
		uniquenessIndexKeyspace: String,
		uniquenessIndexTable: String,
		uniquenessIndexLookup: String
	): Unit = {
		uniqueFieldsRdd.foreachPartition(iter => {
			val mapper = new ObjectMapper()
			mapper.registerModule(DefaultScalaModule)

			iter.foreach(uniqueFields => {

				//// Extract

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

				//// Populate

				var otherId = uniqueFields.otherId
				var username = uniqueFields.username
				var verifiedPhone = uniqueFields.verifiedPhone

				coreEvents.foreach(event => {
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

				contactEvents.foreach(event => {
					if (event.payload.rem.contains("phone")) {
						verifiedPhone = None
					}
					event.payload.set.phone.foreach(newVerifiedPhone => verifiedPhone = Some(newVerifiedPhone))
				})

				//// Write

				if (otherId.nonEmpty)  {
					cc.withSessionDo(session => {
						session.execute(
							s"""
								 |INSERT INTO $uniquenessIndexKeyspace.$uniquenessIndexTable
								 |  ( key, value )
								 |  VALUES (
								 |    '${UniquenessIndexKeyValue.keyFor(UniquenessIndexKeyValue.otherIdLookupKey, otherId.get)}',
								 |    '${uniqueFields.id}' );
							""".stripMargin
						)
					})
					cc.withSessionDo(session => {
						session.execute(
							s"""
								 |INSERT INTO $uniquenessIndexKeyspace.$uniquenessIndexLookup
								 |  ( id, key, value )
								 |  VALUES (
								 |    '${uniqueFields.id}',
								 |    '${UniquenessIndexKeyValue.otherIdLookupKey}',
								 |    '${otherId.get}' );
							""".stripMargin
						)
					})
				}
				if (username.nonEmpty) {
					cc.withSessionDo(session => {
						session.execute(
							s"""
								 |INSERT INTO $uniquenessIndexKeyspace.$uniquenessIndexTable
								 |  ( key, value )
								 |  VALUES (
								 |    '${UniquenessIndexKeyValue.keyFor(UniquenessIndexKeyValue.usernameLookupKey, username.get)}',
								 |    '${uniqueFields.id}' );
							""".stripMargin
						)
					})
					cc.withSessionDo(session => {
						session.execute(
							s"""
								 |INSERT INTO $uniquenessIndexKeyspace.$uniquenessIndexLookup
								 |  ( id, key, value )
								 |  VALUES (
								 |    '${uniqueFields.id}',
								 |    '${UniquenessIndexKeyValue.usernameLookupKey}',
								 |    '${username.get}' );
							""".stripMargin
						)
					})
				}
				if (verifiedPhone.nonEmpty) {
					cc.withSessionDo(session => {
						session.execute(
							s"""
								 |INSERT INTO $uniquenessIndexKeyspace.$uniquenessIndexTable
								 |  ( key, value )
								 |  VALUES (
								 |    '${UniquenessIndexKeyValue.keyFor(UniquenessIndexKeyValue.verifiedPhoneLookupKey, verifiedPhone.get)}',
								 |    '${uniqueFields.id}' );
							""".stripMargin
						)
					})
					cc.withSessionDo(session => {
						session.execute(
							s"""
								 |INSERT INTO $uniquenessIndexKeyspace.$uniquenessIndexLookup
								 |  ( id, key, value )
								 |  VALUES (
								 |    '${uniqueFields.id}',
								 |    '${UniquenessIndexKeyValue.verifiedPhoneLookupKey}',
								 |    '${verifiedPhone.get}' );
							""".stripMargin
						)
					})
				}

			})
		})
	}

}


