package spark.model

import spark.model.Template._

/**
	*
	* @author Adam Martini
	*/
object Common {

	case class UniquenessFields(
		id: String,
		otherIds: List[String] = List(),
		usernames: List[String] = List(),
		verifiedPhones: List[String] = List()
	)
	object UniquenessFields {
		def from(data: UniquenessIndex): UniquenessFields = data match {
			case d if d.key.contains(UniquenessIndexKeyValue.otherIdLookupKey) => UniquenessFields(
				d.value,
				otherIds = List(UniquenessIndexKeyValue.valueFrom(d.key))
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
		def +(a: UniquenessFields, b: UniquenessFields): UniquenessFields = {
			if (a.id != b.id) {
				throw new IllegalArgumentException("ids must match for UniquenessFields combination")
			}
			UniquenessFields(
				a.id,
				a.otherIds ++ b.otherIds,
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
	id: String,
	otherId: Option[String] = None,
	username: Option[String] = None,
	verifiedPhone: Option[String] = None
)
object UniqueFields {
	def from(data: RawEvent) = UniqueFields(data.id)
}