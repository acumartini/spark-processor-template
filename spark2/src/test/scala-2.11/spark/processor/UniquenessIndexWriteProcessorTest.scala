package spark.processor

import com.datastax.spark.connector.cql.CassandraConnector
import org.apache.spark.SparkConf
import org.apache.spark.sql.SaveMode
import spark.cassandra.MockTables.{UniquenessIndexLookupTable, UniquenessIndexTable}
import spark.cassandra.{DBCleanupBeforeAndAfterEach, TestConnector}
import spark.model.Template.{UniquenessIndex, UniquenessIndexKeyValue, UniquenessIndexLookup}
import spark.{AbstractSparkTest, SparkSqlContext}

class UniquenessIndexWriteProcessorTest extends AbstractSparkTest
	with DBCleanupBeforeAndAfterEach
	with SparkSqlContext {

	val appName = "UIWPTest"

	override val conf: SparkConf = sparkConf
		.set("spark.cassandra.connection.host", TestConnector.embeddedHost)
		.set("spark.cassandra.connection.port", TestConnector.embeddedPort.toString)

	override def createTables(): Unit = {
		UniquenessIndexTable.createTable(appName)
		UniquenessIndexLookupTable.createTable(appName)
	}

	override def truncateTables(): Unit = {
		UniquenessIndexTable.truncateTable(appName)
		UniquenessIndexLookupTable.truncateTable(appName)
	}

	behavior of "UniquenessIndexWriteProcessor"

	def id(i: Int = 0) = s"mock:id:$i"
	def otherId(i: Int = 0) = s"mock:otherId:$i"
	def username(i: Int = 0) = s"mock:username:$i"
	def verifiedPhone(i: Int = 0) = s"mock:verifiedPhone:$i"

	def getIdFromUniquenessTable(cc: CassandraConnector, keyspace: String, table: String, key: String): Option[String] = {
		Option(cc.withSessionDo(session => {
			session.execute(
				s"""
					 |SELECT * FROM $keyspace.$table
					 |WHERE key = '$key';
						""".stripMargin
			)
		}).one().getString("value"))
	}

	def getValueFromUniquenessLookupTable(cc: CassandraConnector, keyspace: String, table: String, id: String, key: String): Option[String] = {
		Option(cc.withSessionDo(session => {
			session.execute(
				s"""
					 |SELECT * FROM $keyspace.$table
					 |WHERE id = '$id' AND key = '$key';
					""".stripMargin
			)
		}).one().getString("value"))
	}

	it should "write rows into the target uniqueness index and uniqueness index lookup tables" in {
		val sqlContext = sqx
		import sqlContext.implicits._ // required for implicit Encoders

		Given("a DS of UniquenessIndex and a DS of UniquenessIndexLookup")
		val uniquenessIndexDS = List(
			UniquenessIndex(
				UniquenessIndexKeyValue.keyFor(UniquenessIndexKeyValue.otherIdLookupKey, otherId()),
				id()
			),
			UniquenessIndex(
				UniquenessIndexKeyValue.keyFor(UniquenessIndexKeyValue.usernameLookupKey, username()),
				id()
			),
			UniquenessIndex(
				UniquenessIndexKeyValue.keyFor(UniquenessIndexKeyValue.verifiedPhoneLookupKey, verifiedPhone()),
				id()
			)
		).toDS()

		val uniquenessIndexLookupDS = List(
			UniquenessIndexLookup(
				id(),
				UniquenessIndexKeyValue.otherIdLookupKey,
				otherId()
			),
			UniquenessIndexLookup(
				id(),
				UniquenessIndexKeyValue.usernameLookupKey,
				username()
			),
			UniquenessIndexLookup(
				id(),
				UniquenessIndexKeyValue.verifiedPhoneLookupKey,
				verifiedPhone()
			)
		).toDS()

		When("process uniqueness index and uniqueness index lookup")
		UniquenessIndexWriteProcessor.process(
			uniquenessIndexDS,
			UniquenessIndexTable.keyspaceTableMap(appName),
			uniquenessIndexLookupDS,
			UniquenessIndexLookupTable.keyspaceTableMap(appName)
		)

		val keyspace = UniquenessIndexTable.keyspaceTableMap(appName).getOrElse("keyspace", "")
		val cc = CassandraConnector(sc.getConf)

		getIdFromUniquenessTable(
			cc,
			keyspace,
			UniquenessIndexTable.tableName(appName),
			UniquenessIndexKeyValue.keyFor(UniquenessIndexKeyValue.otherIdLookupKey, otherId())
		) shouldBe Some(id())

		getIdFromUniquenessTable(
			cc,
			keyspace,
			UniquenessIndexTable.tableName(appName),
			UniquenessIndexKeyValue.keyFor(UniquenessIndexKeyValue.usernameLookupKey, username())
		) shouldBe Some(id())

		getIdFromUniquenessTable(
			cc,
			keyspace,
			UniquenessIndexTable.tableName(appName),
			UniquenessIndexKeyValue.keyFor(UniquenessIndexKeyValue.verifiedPhoneLookupKey, verifiedPhone())
		) shouldBe Some(id())

		getValueFromUniquenessLookupTable(
			cc,
			keyspace,
			UniquenessIndexLookupTable.tableName(appName),
			id(),
			UniquenessIndexKeyValue.otherIdLookupKey
		) shouldBe Some(otherId())

		getValueFromUniquenessLookupTable(
			cc,
			keyspace,
			UniquenessIndexLookupTable.tableName(appName),
			id(),
			UniquenessIndexKeyValue.usernameLookupKey
		) shouldBe Some(username())

		getValueFromUniquenessLookupTable(
			cc,
			keyspace,
			UniquenessIndexLookupTable.tableName(appName),
			id(),
			UniquenessIndexKeyValue.verifiedPhoneLookupKey
		) shouldBe Some(verifiedPhone())
	}

	it should "not overwrite existing rows" in {
		val sqlContext = sqx // required for implicit Encoders
		import sqlContext.implicits._ // required for implicit Encoders

		Given("a otherId entry for the user exists in the uniqueness index and lookup tables")
		val existingUniquenessIndexDS = List(
			UniquenessIndex(
				UniquenessIndexKeyValue.keyFor(UniquenessIndexKeyValue.otherIdLookupKey, otherId()),
				id()
			)
		).toDS()

		val existingUniquenessIndexLookupDS = List(
			UniquenessIndexLookup(
				id(),
				UniquenessIndexKeyValue.otherIdLookupKey,
				otherId()
			)
		).toDS()

		existingUniquenessIndexDS.write
			.format("org.apache.spark.sql.cassandra")
			.options(UniquenessIndexTable.keyspaceTableMap(appName))
			.mode(SaveMode.Append)
			.save()

		existingUniquenessIndexLookupDS.write
			.format("org.apache.spark.sql.cassandra")
			.options(UniquenessIndexLookupTable.keyspaceTableMap(appName))
			.mode(SaveMode.Append)
			.save()

		Given("a DS of UniquenessIndex and a DS of UniquenessIndexLookup")
		val uniquenessIndexDS = List(
			UniquenessIndex(
				UniquenessIndexKeyValue.keyFor(UniquenessIndexKeyValue.otherIdLookupKey, otherId(1)),
				id()
			),
			UniquenessIndex(
				UniquenessIndexKeyValue.keyFor(UniquenessIndexKeyValue.otherIdLookupKey, otherId(2)),
				id(1)
			)
		).toDS()

		val uniquenessIndexLookupDS = List(
			UniquenessIndexLookup(
				id(),
				UniquenessIndexKeyValue.otherIdLookupKey,
				otherId(1)
			),
			UniquenessIndexLookup(
				id(1),
				UniquenessIndexKeyValue.otherIdLookupKey,
				otherId(2)
			)
		).toDS()

		When("process uniqueness index and uniqueness index lookup")
		UniquenessIndexWriteProcessor.process(
			uniquenessIndexDS,
			UniquenessIndexTable.keyspaceTableMap(appName),
			uniquenessIndexLookupDS,
			UniquenessIndexLookupTable.keyspaceTableMap(appName)
		)

		val keyspace = UniquenessIndexTable.keyspaceTableMap(appName).getOrElse("keyspace", "")
		val cc = CassandraConnector(sc.getConf)

		Then("existing data is overwritten")
		getIdFromUniquenessTable(
			cc,
			keyspace,
			UniquenessIndexTable.tableName(appName),
			UniquenessIndexKeyValue.keyFor(UniquenessIndexKeyValue.otherIdLookupKey, otherId(1))
		) shouldBe Some(id())
		getValueFromUniquenessLookupTable(
			cc,
			keyspace,
			UniquenessIndexLookupTable.tableName(appName),
			id(),
			UniquenessIndexKeyValue.otherIdLookupKey
		) shouldBe Some(otherId(1))

		Then("new data is added")
		getIdFromUniquenessTable(
			cc,
			keyspace,
			UniquenessIndexTable.tableName(appName),
			UniquenessIndexKeyValue.keyFor(UniquenessIndexKeyValue.otherIdLookupKey, otherId(2))
		) shouldBe Some(id(1))
		getValueFromUniquenessLookupTable(
			cc,
			keyspace,
			UniquenessIndexLookupTable.tableName(appName),
			id(1),
			UniquenessIndexKeyValue.otherIdLookupKey
		) shouldBe Some(otherId(2))
	}

}
