package spark.processor

import com.datastax.driver.core.Row
import com.datastax.spark.connector.cql.CassandraConnector
import org.apache.spark.rdd.RDD
import spark.model.Template.{UniquenessIndex, UniquenessIndexKeyValue, UniquenessIndexLookup}
import spark.model.UniqueFields

import scala.util.{Failure, Success, Try}

/**
	* Accepts a RDD of UniqueFields and return RDDs of UniquenessIndex and UniquenessIndexLookup that can be used to
	* update the uniqueness index tables. Also returns an RDD of UniquenessIndexStatus which indicates the status of
	* each uniqueness field in the existing uniqueness index tables wrt the source of truth.
	*
	* @return
	*/
object UniquenessIndexProcessor {

	def process(
		data: RDD[UniqueFields],
		cc: CassandraConnector,
		sourceUniquenessKeyspace: String,
		sourceUniquenessTable: String,
		sourceUniquenessLookupTable: String,
		generateStatus: Boolean = true
	): (RDD[UniquenessIndex], RDD[UniquenessIndexLookup], Option[RDD[UniquenessIndexStatus]]) = {

		def uniquenessTableFieldStatus(key: String, id: String) = Try(
			Option(cc.withSessionDo(session => {
				session.execute(
					s"""
						|SELECT * FROM $sourceUniquenessKeyspace.$sourceUniquenessTable
						|WHERE key = '${cleanKey(key)}';
					""".stripMargin
				)
			}).one)
		) match {
			case Success(row: Option[Row]) => row.map(row => UniquenessIndex(
				row.getString("key"),
				row.getString("value")
			))
				.map(uniquenessIndex => {
					if (uniquenessIndex.value == id) {
						UniqueFieldStatus.pass
					} else {
						UniqueFieldStatus(
							pass = false,
							UniqueFieldStatus.conflict(uniquenessIndex.value, id)
						)
					}
				})
				.getOrElse(UniqueFieldStatus(reason = UniqueFieldStatus.missing))
			case Failure(_) => UniqueFieldStatus(reason = UniqueFieldStatus.queryException)
		}

		def uniquenessLookupTableFieldStatus(id: String, key: String, value: String) = Try(
			Option(cc.withSessionDo(session => {
				session.execute(
					s"""
						|SELECT * FROM $sourceUniquenessKeyspace.$sourceUniquenessLookupTable
						|WHERE id = '$id' AND key = '${cleanKey(key)}';
					""".stripMargin
				)
			}).one)
		) match {
			case Success(row: Option[Row]) => row.map(row => UniquenessIndexLookup(
				row.getString("id"),
				row.getString("key"),
				row.getString("value")
			))
				.map(uniquenessIndex => {
					if (uniquenessIndex.value == value) {
						UniqueFieldStatus.pass
					} else {
						UniqueFieldStatus(
							pass = false,
							UniqueFieldStatus.conflict(uniquenessIndex.value, value)
						)
					}
				})
				.getOrElse(UniqueFieldStatus(reason = UniqueFieldStatus.missing))
			case Failure(_) => UniqueFieldStatus(reason = UniqueFieldStatus.queryException)
		}

		var status = Option.empty[RDD[UniquenessIndexStatus]]
		if (generateStatus) status = Some(data.map(uniqueFields => {
			UniquenessIndexStatus(
				uniqueFields.id,
				uniqueFields.otherId.map(otherIdField => {
					uniquenessTableFieldStatus(
						UniquenessIndexKeyValue.keyFor(UniquenessIndexKeyValue.otherIdLookupKey, otherIdField),
						uniqueFields.id
					)
				}),
				uniqueFields.otherId.map(otherIdField => {
					uniquenessLookupTableFieldStatus(
						uniqueFields.id,
						UniquenessIndexKeyValue.otherIdLookupKey,
						otherIdField
					)
				}),
				uniqueFields.username.map(usernameField => {
					uniquenessTableFieldStatus(
						UniquenessIndexKeyValue.keyFor(UniquenessIndexKeyValue.usernameLookupKey, usernameField),
						uniqueFields.id
					)
				}),
				uniqueFields.username.map(usernameField => {
					uniquenessLookupTableFieldStatus(
						uniqueFields.id,
						UniquenessIndexKeyValue.usernameLookupKey,
						usernameField
					)
				}),
				uniqueFields.verifiedPhone.map(verifiedPhoneField => {
					uniquenessTableFieldStatus(
						UniquenessIndexKeyValue.keyFor(UniquenessIndexKeyValue.verifiedPhoneLookupKey, verifiedPhoneField),
						uniqueFields.id
					)
				}),
				uniqueFields.verifiedPhone.map(verifiedPhoneField => {
					uniquenessLookupTableFieldStatus(
						uniqueFields.id,
						UniquenessIndexKeyValue.verifiedPhoneLookupKey,
						verifiedPhoneField
					)
				})
			)
		}))

		(
			data.flatMap(uniqueFields => List(
				uniqueFields.otherId.map(otherId => UniquenessIndex(
					UniquenessIndexKeyValue.keyFor(UniquenessIndexKeyValue.otherIdLookupKey, otherId),
					uniqueFields.id
				)),
				uniqueFields.username.map(username => UniquenessIndex(
					UniquenessIndexKeyValue.keyFor(UniquenessIndexKeyValue.usernameLookupKey, username),
					uniqueFields.id
				)),
				uniqueFields.verifiedPhone.map(verifiedPhone => UniquenessIndex(
					UniquenessIndexKeyValue.keyFor(UniquenessIndexKeyValue.verifiedPhoneLookupKey, verifiedPhone),
					uniqueFields.id
				))).flatten
			),
			data.flatMap(uniqueFields => List(
				uniqueFields.otherId.map(otherId => UniquenessIndexLookup(
					uniqueFields.id,
					UniquenessIndexKeyValue.otherIdLookupKey,
					otherId
				)),
				uniqueFields.username.map(username => UniquenessIndexLookup(
					uniqueFields.id,
					UniquenessIndexKeyValue.usernameLookupKey,
					username
				)),
				uniqueFields.verifiedPhone.map(verifiedPhone => UniquenessIndexLookup(
					uniqueFields.id,
					UniquenessIndexKeyValue.verifiedPhoneLookupKey,
					verifiedPhone
				))).flatten
			),
			status
		)
	}

	def cleanKey(key: String): String = key.replaceAll("'", "''")

}

case class UniqueFieldStatus(
	pass: Boolean = false,
	reason: String
) {
	def toCsvString: String = reason
}
object UniqueFieldStatus {
	val pass = UniqueFieldStatus(pass = true, "Pass")
	val missing = "Missing"
	val queryException = "Query Exception"
	def conflict(existing: String, populated: String) = s"Conflict: existing [$existing] populated [$populated]"
}
case class UniquenessIndexStatus(
	id: String,
	otherId: Option[UniqueFieldStatus],
	otherIdLookup: Option[UniqueFieldStatus],
	username: Option[UniqueFieldStatus],
	usernameLookup: Option[UniqueFieldStatus],
	verifiedPhone: Option[UniqueFieldStatus],
	verifiedPhoneLookup: Option[UniqueFieldStatus]
) {
	def toCsvString: String = List(
		id,
		otherId.map(_.toCsvString).getOrElse(""),
		otherIdLookup.map(_.toCsvString).getOrElse(""),
		username.map(_.toCsvString).getOrElse(""),
		usernameLookup.map(_.toCsvString).getOrElse(""),
		verifiedPhone.map(_.toCsvString).getOrElse(""),
		verifiedPhoneLookup.map(_.toCsvString).getOrElse("")
	).mkString(", ")
}
