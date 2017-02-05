package spark.processor

import com.datastax.spark.connector.cql.CassandraConnector
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import org.apache.spark.rdd.RDD
import spark.model.Identity._
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

				//// Populate

				var nuId = uniqueFields.nuId
				var username = uniqueFields.username
				var verifiedPhone = uniqueFields.verifiedPhone

				coreEvents.foreach(event => {
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

				contactEvents.foreach(event => {
					if (event.payload.rem.contains("verifiedphone")) {
						verifiedPhone = None
					}
					event.payload.set.verifiedphone.foreach(newVerifiedPhone => verifiedPhone = Some(newVerifiedPhone))
				})

				//// Write

				if (nuId.nonEmpty)  {
					cc.withSessionDo(session => {
						session.execute(
							s"""
								 |INSERT INTO $uniquenessIndexKeyspace.$uniquenessIndexTable
								 |  ( key, value )
								 |  VALUES (
								 |    '${UniquenessIndexKeyValue.keyFor(UniquenessIndexKeyValue.nuIdLookupKey, nuId.get)}',
								 |    '${uniqueFields.upmId}' );
							""".stripMargin
						)
					})
					cc.withSessionDo(session => {
						session.execute(
							s"""
								 |INSERT INTO $uniquenessIndexKeyspace.$uniquenessIndexLookup
								 |  ( id, key, value )
								 |  VALUES (
								 |    '${uniqueFields.upmId}',
								 |    '${UniquenessIndexKeyValue.nuIdLookupKey}',
								 |    '${nuId.get}' );
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
								 |    '${uniqueFields.upmId}' );
							""".stripMargin
						)
					})
					cc.withSessionDo(session => {
						session.execute(
							s"""
								 |INSERT INTO $uniquenessIndexKeyspace.$uniquenessIndexLookup
								 |  ( id, key, value )
								 |  VALUES (
								 |    '${uniqueFields.upmId}',
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
								 |    '${uniqueFields.upmId}' );
							""".stripMargin
						)
					})
					cc.withSessionDo(session => {
						session.execute(
							s"""
								 |INSERT INTO $uniquenessIndexKeyspace.$uniquenessIndexLookup
								 |  ( id, key, value )
								 |  VALUES (
								 |    '${uniqueFields.upmId}',
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


