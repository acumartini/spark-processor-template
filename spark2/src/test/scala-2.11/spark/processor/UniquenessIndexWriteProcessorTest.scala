package spark.processor

import com.datastax.spark.connector.cql.CassandraConnector
import org.apache.spark.SparkConf
import org.apache.spark.sql.SaveMode
import spark.cassandra.MockTables.{UniquenessIndexLookupTable, UniquenessIndexTable}
import spark.cassandra.{DBCleanupBeforeAndAfterEach, TestConnector}
import spark.model.Identity.{UniquenessIndex, UniquenessIndexKeyValue, UniquenessIndexLookup}
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

	def upmId(i: Int = 0) = s"mock:upmId:$i"
	def nuId(i: Int = 0) = s"mock:nuId:$i"
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
				UniquenessIndexKeyValue.keyFor(UniquenessIndexKeyValue.nuIdLookupKey, nuId()),
				upmId()
			),
			UniquenessIndex(
				UniquenessIndexKeyValue.keyFor(UniquenessIndexKeyValue.usernameLookupKey, username()),
				upmId()
			),
			UniquenessIndex(
				UniquenessIndexKeyValue.keyFor(UniquenessIndexKeyValue.verifiedPhoneLookupKey, verifiedPhone()),
				upmId()
			)
		).toDS()

		val uniquenessIndexLookupDS = List(
			UniquenessIndexLookup(
				upmId(),
				UniquenessIndexKeyValue.nuIdLookupKey,
				nuId()
			),
			UniquenessIndexLookup(
				upmId(),
				UniquenessIndexKeyValue.usernameLookupKey,
				username()
			),
			UniquenessIndexLookup(
				upmId(),
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
			UniquenessIndexKeyValue.keyFor(UniquenessIndexKeyValue.nuIdLookupKey, nuId())
		) shouldBe Some(upmId())

		getIdFromUniquenessTable(
			cc,
			keyspace,
			UniquenessIndexTable.tableName(appName),
			UniquenessIndexKeyValue.keyFor(UniquenessIndexKeyValue.usernameLookupKey, username())
		) shouldBe Some(upmId())

		getIdFromUniquenessTable(
			cc,
			keyspace,
			UniquenessIndexTable.tableName(appName),
			UniquenessIndexKeyValue.keyFor(UniquenessIndexKeyValue.verifiedPhoneLookupKey, verifiedPhone())
		) shouldBe Some(upmId())

		getValueFromUniquenessLookupTable(
			cc,
			keyspace,
			UniquenessIndexLookupTable.tableName(appName),
			upmId(),
			UniquenessIndexKeyValue.nuIdLookupKey
		) shouldBe Some(nuId())

		getValueFromUniquenessLookupTable(
			cc,
			keyspace,
			UniquenessIndexLookupTable.tableName(appName),
			upmId(),
			UniquenessIndexKeyValue.usernameLookupKey
		) shouldBe Some(username())

		getValueFromUniquenessLookupTable(
			cc,
			keyspace,
			UniquenessIndexLookupTable.tableName(appName),
			upmId(),
			UniquenessIndexKeyValue.verifiedPhoneLookupKey
		) shouldBe Some(verifiedPhone())
	}

	it should "not overwrite existing rows" in {
		val sqlContext = sqx // required for implicit Encoders
		import sqlContext.implicits._ // required for implicit Encoders

		Given("a nuId entry for the user exists in the uniqueness index and lookup tables")
		val existingUniquenessIndexDS = List(
			UniquenessIndex(
				UniquenessIndexKeyValue.keyFor(UniquenessIndexKeyValue.nuIdLookupKey, nuId()),
				upmId()
			)
		).toDS()

		val existingUniquenessIndexLookupDS = List(
			UniquenessIndexLookup(
				upmId(),
				UniquenessIndexKeyValue.nuIdLookupKey,
				nuId()
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
				UniquenessIndexKeyValue.keyFor(UniquenessIndexKeyValue.nuIdLookupKey, nuId(1)),
				upmId()
			),
			UniquenessIndex(
				UniquenessIndexKeyValue.keyFor(UniquenessIndexKeyValue.nuIdLookupKey, nuId(2)),
				upmId(1)
			)
		).toDS()

		val uniquenessIndexLookupDS = List(
			UniquenessIndexLookup(
				upmId(),
				UniquenessIndexKeyValue.nuIdLookupKey,
				nuId(1)
			),
			UniquenessIndexLookup(
				upmId(1),
				UniquenessIndexKeyValue.nuIdLookupKey,
				nuId(2)
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
			UniquenessIndexKeyValue.keyFor(UniquenessIndexKeyValue.nuIdLookupKey, nuId(1))
		) shouldBe Some(upmId())
		getValueFromUniquenessLookupTable(
			cc,
			keyspace,
			UniquenessIndexLookupTable.tableName(appName),
			upmId(),
			UniquenessIndexKeyValue.nuIdLookupKey
		) shouldBe Some(nuId(1))

		Then("new data is added")
		getIdFromUniquenessTable(
			cc,
			keyspace,
			UniquenessIndexTable.tableName(appName),
			UniquenessIndexKeyValue.keyFor(UniquenessIndexKeyValue.nuIdLookupKey, nuId(2))
		) shouldBe Some(upmId(1))
		getValueFromUniquenessLookupTable(
			cc,
			keyspace,
			UniquenessIndexLookupTable.tableName(appName),
			upmId(1),
			UniquenessIndexKeyValue.nuIdLookupKey
		) shouldBe Some(nuId(2))
	}

}
