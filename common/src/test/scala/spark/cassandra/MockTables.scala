package spark.cassandra

import java.util.UUID

import com.datastax.driver.core.Session

/**
	*
	* @author Adam Martini
	*/
trait MockTable {

	protected def tablePrefix: String
	def tableName(id: String): String = s"$tablePrefix${id.toLowerCase}"
	def keyspaceTableMap(id: String) = Map(
		"keyspace" -> TestConnector.embeddedKeySpace,
		"table" -> tableName(id)
	)
	def createTable(id: String)(implicit session: Session)
	def truncateTable(id: String)(implicit session: Session): Unit = session.execute(
		s"TRUNCATE ${TestConnector.embeddedKeySpace}.${tableName(id)};"
	)

}

object MockTables {

	trait MockEvents extends MockTable {
		def createTable(id: String)(implicit session: Session): Unit = session.execute(
			s"""
				 |CREATE TABLE IF NOT EXISTS ${TestConnector.embeddedKeySpace}.${tableName(id)} (
				 |  id text,
				 |  time timeuuid,
				 |  payload text,
				 |  PRIMARY KEY ((id), time)
				 |);
			""".stripMargin
		)
	}
	object CoreEvents extends MockEvents {
		protected val tablePrefix = "core_events"
	}
	object ContactEvents extends MockEvents {
		protected val tablePrefix = "contact_events"
	}

	object UniquenessIndexTable extends MockTable {
		protected val tablePrefix = "uniqueness_index"
		def createTable(id: String)(implicit session: Session): Unit = session.execute(
			s"""
				|CREATE TABLE IF NOT EXISTS ${TestConnector.embeddedKeySpace}.${tableName(id)} (
				|  key text PRIMARY KEY,
				|  value text
				|);
			""".stripMargin
		)
	}
	object UniquenessIndexLookupTable extends MockTable {
		protected val tablePrefix = "uniqueness_index_lookup"
		def createTable(id: String)(implicit session: Session): Unit = session.execute(
			s"""
				|CREATE TABLE IF NOT EXISTS ${TestConnector.embeddedKeySpace}.${tableName(id)} (
				|  id text,
				|  key text,
				|  value text,
				|  PRIMARY KEY ((id), key)
				|);
			""".stripMargin
		)
	}

}

// Note: Moved the following case classes out of their enclosing MockTables parent object due to the limitations of
// implicit DataSet encoders in spark-sql_1.6.1. Parent classes are handled well in spark-sql_2.x.

case class KeyValue(
	key: String,
	value: String
)
object KeyValue extends MockTable {
	protected val tablePrefix = "kv"
	def createTable(id: String)(implicit session: Session): Unit = session.execute(
		s"""
			|CREATE TABLE IF NOT EXISTS ${TestConnector.embeddedKeySpace}.${tableName(id)} (
			|  key text PRIMARY KEY,
			|  value int
			|);
		""".stripMargin
	)
}

case class User(
	id: String = UUID.randomUUID().toString,
	firstName: String,
	lastName: String
)
object User extends MockTable {
	protected val tablePrefix = "user"
	def createTable(id: String)(implicit session: Session): Unit = session.execute(
		s"""
			|CREATE TABLE IF NOT EXISTS ${TestConnector.embeddedKeySpace}.${tableName(id)} (
			|  id text PRIMARY KEY,
			|  "firstName" text,
			|  "lastName" text
			|);
		""".stripMargin
	)
}
