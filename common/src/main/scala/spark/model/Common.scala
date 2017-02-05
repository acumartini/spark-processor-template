package spark.model

import spark.model.Identity._

/**
	*
	* @author Adam Martini
	*/
object Common {

	case class UniquenessFields(
		upmId: String,
		nuIds: List[String] = List(),
		usernames: List[String] = List(),
		verifiedPhones: List[String] = List()
	)
	object UniquenessFields {
		def from(data: UniquenessIndex): UniquenessFields = data match {
			case d if d.key.contains(UniquenessIndexKeyValue.nuIdLookupKey) => UniquenessFields(
				d.value,
				nuIds = List(UniquenessIndexKeyValue.valueFrom(d.key))
			)
			case d if d.key.contains(UniquenessIndexKeyValue.usernameLookupKey) => UniquenessFields(
				d.value,
				usernames = List(UniquenessIndexKeyValue.valueFrom(d.key))
			)
			case d if d.key.contains(UniquenessIndexKeyValue.verifiedPhoneLookupKey) => UniquenessFields(
				d.value,
				verifiedPhones = List(UniquenessIndexKeyValue.valueFrom(d.key))
			)
		}
		def +(a: UniquenessFields, b: UniquenessFields) = {
			if (a.upmId != b.upmId) {
				throw new IllegalArgumentException("upmIds must match for UniquenessFields combination")
			}
			UniquenessFields(
				a.upmId,
				a.nuIds ++ b.nuIds,
				a.usernames ++ b.usernames,
				a.verifiedPhones ++ b.verifiedPhones
			)
		}
	}

}

// Note: Moved the following case classes out of their enclosing Common parent object due to the limitations of
// implicit DataSet encoders in spark-sql_1.6.1. Parent classes are handled well in spark-sql_2.x.

// Helper class used to aggregate fields which are considered *unique* across all models
case class UniqueFields(
	upmId: String,
	nuId: Option[String] = None,
	username: Option[String] = None,
	verifiedPhone: Option[String] = None
)
object UniqueFields {
	def from(data: IdentityOmega) = UniqueFields(data.user_upm_id)
	def from(data: RawIdentityEvent) = UniqueFields(data.id)
}