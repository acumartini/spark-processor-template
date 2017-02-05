package spark.processor

import com.datastax.spark.connector.cql.CassandraConnector
import org.apache.spark.SparkConf
import org.apache.spark.sql.SaveMode
import spark.cassandra.MockTables.{UniquenessIndexLookupTable, UniquenessIndexTable}
import spark.cassandra.{DBCleanupBeforeAndAfterEach, TestConnector}
import spark.model.Template.{UniquenessIndex, UniquenessIndexKeyValue, UniquenessIndexLookup}
import spark.model.UniqueFields
import spark.{AbstractSparkTest, SparkSqlContext}

/**
	*
	* @author Adam Martini
	*/
class UniquenessIndexProcessorTest extends AbstractSparkTest
	with DBCleanupBeforeAndAfterEach
	with SparkSqlContext {

	val appName = "UIPTest"

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

	behavior of "UniquenessIndexProcessor"

	def id(i: Int = 0) = s"mock:id:$i"
	def otherId(i: Int = 0) = s"mock:otherId:$i"
	def username(i: Int = 0) = s"mock:username:$i"
	def verifiedPhone(i: Int = 0) = s"mock:verifiedPhone:$i"
	def extractIdIndex(id: String): Int = id.substring(id.lastIndexOf(":") + 1).toInt
	def corrupt(i: Int = 0): String = s"mock:corrupt:':$i"

	it should "return an RDD[UniquenessIndex], an RDD[UniquenessIndexLookup], and an RDD[UniquenessIndexStatus]" in {
		val sqlContext = sqx
		import sqlContext.implicits._ // required for implicit Encoders

		val cc = CassandraConnector(sc.getConf)

		Given("an list of UniqueFields objects")
		val uniqueFieldsObjs = (0 until 100).map(i => UniqueFields(
			id(i),
			Some(otherId(i)),
			Some(username(i)),
			Some(verifiedPhone(i))
		))

		Given("existing uniqueness index and uniqueness index lookup entries for each UniqueFields object")
		val uniquenessIndexDS = (0 until 100).flatMap(i => List(
			UniquenessIndex(
				UniquenessIndexKeyValue.keyFor(UniquenessIndexKeyValue.otherIdLookupKey, otherId(i)),
				id(i)
			),
			UniquenessIndex(
				UniquenessIndexKeyValue.keyFor(UniquenessIndexKeyValue.usernameLookupKey, username(i)),
				id(i)
			),
			UniquenessIndex(
				UniquenessIndexKeyValue.keyFor(UniquenessIndexKeyValue.verifiedPhoneLookupKey, verifiedPhone(i)),
				id(i)
			)
		)).toDS()

		val uniquenessIndexLookupDS = (0 until 100).flatMap(i => List(
			UniquenessIndexLookup(
				id(i),
				UniquenessIndexKeyValue.otherIdLookupKey,
				otherId(i)
			),
			UniquenessIndexLookup(
				id(i),
				UniquenessIndexKeyValue.usernameLookupKey,
				username(i)
			),
			UniquenessIndexLookup(
				id(i),
				UniquenessIndexKeyValue.verifiedPhoneLookupKey,
				verifiedPhone(i)
			)
		)).toDS()

		uniquenessIndexDS.write
			.format("org.apache.spark.sql.cassandra")
			.options(UniquenessIndexTable.keyspaceTableMap(appName))
			.mode(SaveMode.Append)
			.save()

		uniquenessIndexLookupDS.write
			.format("org.apache.spark.sql.cassandra")
			.options(UniquenessIndexLookupTable.keyspaceTableMap(appName))
			.mode(SaveMode.Append)
			.save()

		When("processing UniqueFields objects")
		sc.parallelize(uniqueFieldsObjs)
		val (uniquenessIndexRDD, uniquenessLookupIndexRDD, uniquenessIndexStatusRDD) = UniquenessIndexProcessor.process(
			sc.parallelize(uniqueFieldsObjs),
			cc,
			TestConnector.embeddedKeySpace,
			UniquenessIndexTable.tableName(appName),
			UniquenessIndexLookupTable.tableName(appName)
		)

		uniquenessIndexRDD.count() shouldEqual 300
		uniquenessIndexRDD.collect
			.groupBy(_.value)
			.values
			.foreach(uniquenessIndices => {
				uniquenessIndices.length shouldEqual 3
				val i = extractIdIndex(uniquenessIndices.head.value)
				uniquenessIndices.exists(_.key.contains(otherId(i))) shouldBe true
				uniquenessIndices.exists(_.key.contains(username(i))) shouldBe true
				uniquenessIndices.exists(_.key.contains(verifiedPhone(i))) shouldBe true
			})

		uniquenessLookupIndexRDD.count() shouldEqual 300
		uniquenessLookupIndexRDD.collect
			.groupBy(_.id)
			.values
			.foreach(uniquenessLookupIndices => {
				uniquenessLookupIndices.length shouldEqual 3
				val i = extractIdIndex(uniquenessLookupIndices.head.id)
				uniquenessLookupIndices.exists(_.value.contains(otherId(i))) shouldBe true
				uniquenessLookupIndices.exists(_.value.contains(username(i))) shouldBe true
				uniquenessLookupIndices.exists(_.value.contains(verifiedPhone(i))) shouldBe true
			})

		uniquenessIndexStatusRDD.foreach(_.count() shouldEqual 100)
		def verifyFieldStatus = (fieldStatus: UniqueFieldStatus) => fieldStatus shouldEqual UniqueFieldStatus.pass
		uniquenessIndexStatusRDD.foreach(_.collect.foreach(status => {
			status.otherId.foreach(verifyFieldStatus)
			status.otherIdLookup.foreach(verifyFieldStatus)
			status.username.foreach(verifyFieldStatus)
			status.usernameLookup.foreach(verifyFieldStatus)
			status.verifiedPhone.foreach(verifyFieldStatus)
			status.verifiedPhoneLookup.foreach(verifyFieldStatus)
		}))
	}

	it should "handle user missing unique field in uniqueness index and uniqueness index lookup" in {
		val cc = CassandraConnector(sc.getConf)

		Given("an list of UniqueFields objects")
		val uniqueFieldsObjs = Seq(UniqueFields(
			id(),
			Some(otherId()),
			Some(username()),
			Some(verifiedPhone())
		))

		When("processing UniqueFields objects")
		val (uniquenessIndexRDD, uniquenessLookupIndexRDD, uniquenessIndexStatusRDD) = UniquenessIndexProcessor.process(
			sc.parallelize(uniqueFieldsObjs),
			cc,
			TestConnector.embeddedKeySpace,
			UniquenessIndexTable.tableName(appName),
			UniquenessIndexLookupTable.tableName(appName)
		)

		uniquenessIndexRDD.count() shouldEqual 3
		uniquenessIndexRDD.collect
			.groupBy(_.value)
			.values
			.foreach(uniquenessIndices => {
				uniquenessIndices.length shouldEqual 3
				uniquenessIndices.exists(_.key.contains(otherId())) shouldBe true
				uniquenessIndices.exists(_.key.contains(username())) shouldBe true
				uniquenessIndices.exists(_.key.contains(verifiedPhone())) shouldBe true
			})

		uniquenessLookupIndexRDD.count() shouldEqual 3
		uniquenessLookupIndexRDD.collect
			.groupBy(_.id)
			.values
			.foreach(uniquenessLookupIndices => {
				uniquenessLookupIndices.length shouldEqual 3
				uniquenessLookupIndices.exists(_.value.contains(otherId())) shouldBe true
				uniquenessLookupIndices.exists(_.value.contains(username())) shouldBe true
				uniquenessLookupIndices.exists(_.value.contains(verifiedPhone())) shouldBe true
			})

		uniquenessIndexStatusRDD.foreach(_.count() shouldEqual 1)
		def verifyFieldStatus = (fieldStatus: UniqueFieldStatus) => {
			fieldStatus.pass shouldEqual false
			fieldStatus.reason shouldEqual UniqueFieldStatus.missing
		}
		uniquenessIndexStatusRDD.foreach(_.collect.foreach(status => {
			status.otherId.foreach(verifyFieldStatus)
			status.otherIdLookup.foreach(verifyFieldStatus)
			status.username.foreach(verifyFieldStatus)
			status.usernameLookup.foreach(verifyFieldStatus)
			status.verifiedPhone.foreach(verifyFieldStatus)
			status.verifiedPhoneLookup.foreach(verifyFieldStatus)
		}))
	}

	it should "handle conflicting ownership of a unique field" in {
		val sqlContext = sqx // required for implicit Encoders
		import sqlContext.implicits._ // required for implicit Encoders

		val cc = CassandraConnector(sc.getConf)

		Given("an list of UniqueFields objects")
		val uniqueFieldsObjs = Seq(UniqueFields(
			id(),
			Some(otherId()),
			Some(username()),
			Some(verifiedPhone())
		))

		Given("existing uniqueness index and uniqueness index lookup entries with conflicting data")
		val uniquenessIndexDS = List(
			UniquenessIndex(
				UniquenessIndexKeyValue.keyFor(UniquenessIndexKeyValue.otherIdLookupKey, otherId()),
				id(1)
			),
			UniquenessIndex(
				UniquenessIndexKeyValue.keyFor(UniquenessIndexKeyValue.usernameLookupKey, username()),
				id(1)
			),
			UniquenessIndex(
				UniquenessIndexKeyValue.keyFor(UniquenessIndexKeyValue.verifiedPhoneLookupKey, verifiedPhone()),
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
				id(),
				UniquenessIndexKeyValue.usernameLookupKey,
				username(1)
			),
			UniquenessIndexLookup(
				id(),
				UniquenessIndexKeyValue.verifiedPhoneLookupKey,
				verifiedPhone(1)
			)
		).toDS()

		uniquenessIndexDS.write
			.format("org.apache.spark.sql.cassandra")
			.options(UniquenessIndexTable.keyspaceTableMap(appName))
			.mode(SaveMode.Append)
			.save()

		uniquenessIndexLookupDS.write
			.format("org.apache.spark.sql.cassandra")
			.options(UniquenessIndexLookupTable.keyspaceTableMap(appName))
			.mode(SaveMode.Append)
			.save()

		When("processing UniqueFields objects")
		sc.parallelize(uniqueFieldsObjs)
		val (uniquenessIndexRDD, uniquenessLookupIndexRDD, uniquenessIndexStatusRDD) = UniquenessIndexProcessor.process(
			sc.parallelize(uniqueFieldsObjs),
			cc,
			TestConnector.embeddedKeySpace,
			UniquenessIndexTable.tableName(appName),
			UniquenessIndexLookupTable.tableName(appName)
		)

		uniquenessIndexRDD.count() shouldEqual 3
		uniquenessIndexRDD.collect
			.groupBy(_.value)
			.values
			.foreach(uniquenessIndices => {
				uniquenessIndices.length shouldEqual 3
				uniquenessIndices.exists(_.key.contains(otherId())) shouldBe true
				uniquenessIndices.exists(_.key.contains(username())) shouldBe true
				uniquenessIndices.exists(_.key.contains(verifiedPhone())) shouldBe true
			})

		uniquenessLookupIndexRDD.count() shouldEqual 3
		uniquenessLookupIndexRDD.collect
			.groupBy(_.id)
			.values
			.foreach(uniquenessLookupIndices => {
				uniquenessLookupIndices.length shouldEqual 3
				uniquenessLookupIndices.exists(_.value.contains(otherId())) shouldBe true
				uniquenessLookupIndices.exists(_.value.contains(username())) shouldBe true
				uniquenessLookupIndices.exists(_.value.contains(verifiedPhone())) shouldBe true
			})

		uniquenessIndexStatusRDD.foreach(_.count() shouldEqual 1)
		def verifyFieldStatus = (fieldStatus: UniqueFieldStatus, existing: String, populated: String) => {
			fieldStatus.pass shouldEqual false
			fieldStatus.reason shouldEqual UniqueFieldStatus.conflict(existing, populated)
		}
		uniquenessIndexStatusRDD.foreach(_.collect.foreach(status => {
			status.otherId.foreach(verifyFieldStatus(_, id(1), id()))
			status.otherIdLookup.foreach(verifyFieldStatus(_, otherId(1), otherId()))
			status.username.foreach(verifyFieldStatus(_, id(1), id()))
			status.usernameLookup.foreach(verifyFieldStatus(_, username(1), username()))
			status.verifiedPhone.foreach(verifyFieldStatus(_, id(1), id()))
			status.verifiedPhoneLookup.foreach(verifyFieldStatus(_, verifiedPhone(1), verifiedPhone()))
		}))
	}

	it should "handle users who do not have one or more of the unique fields in the source of truth" in {
		val cc = CassandraConnector(sc.getConf)

		Given("an list of UniqueFields objects")
		val uniqueFieldsObjs = Seq(UniqueFields(id()))

		When("processing UniqueFields objects")
		sc.parallelize(uniqueFieldsObjs)
		val (uniquenessIndexRDD, uniquenessLookupIndexRDD, uniquenessIndexStatusRDD) = UniquenessIndexProcessor.process(
			sc.parallelize(uniqueFieldsObjs),
			cc,
			TestConnector.embeddedKeySpace,
			UniquenessIndexTable.tableName(appName),
			UniquenessIndexLookupTable.tableName(appName)
		)

		uniquenessIndexRDD.count() shouldEqual 0
		uniquenessLookupIndexRDD.count() shouldEqual 0
		uniquenessIndexStatusRDD.foreach(_.count() shouldEqual 1)
		uniquenessIndexStatusRDD.foreach(_.collect.foreach(status => {
			status.otherId shouldBe None
			status.otherIdLookup shouldBe None
			status.username shouldBe None
			status.usernameLookup shouldBe None
			status.verifiedPhone shouldBe None
			status.verifiedPhoneLookup shouldBe None
		}))
	}

	it should "handle users with single quotes in username" in {
		val sqlContext = sqx // required for implicit Encoders
		import sqlContext.implicits._ // required for implicit Encoders

		val cc = CassandraConnector(sc.getConf)

		Given("an list of UniqueFields objects with corrupt data")
		val uniqueFieldsObjs = Seq(UniqueFields(
			id(),
			username = Some(corrupt())
		))

		Given("existing uniqueness index and uniqueness index lookup entries with conflicting data")
		val uniquenessIndexDS = List(
			UniquenessIndex(
				UniquenessIndexKeyValue.keyFor(UniquenessIndexKeyValue.usernameLookupKey, corrupt()),
				id()
			)
		).toDS()

		val uniquenessIndexLookupDS = List(
			UniquenessIndexLookup(
				id(),
				UniquenessIndexKeyValue.usernameLookupKey,
				corrupt()
			)
		).toDS()

		uniquenessIndexDS.write
			.format("org.apache.spark.sql.cassandra")
			.options(UniquenessIndexTable.keyspaceTableMap(appName))
			.mode(SaveMode.Append)
			.save()

		uniquenessIndexLookupDS.write
			.format("org.apache.spark.sql.cassandra")
			.options(UniquenessIndexLookupTable.keyspaceTableMap(appName))
			.mode(SaveMode.Append)
			.save()

		When("processing UniqueFields objects")
		val (uniquenessIndexRDD, uniquenessLookupIndexRDD, uniquenessIndexStatusRDD) = UniquenessIndexProcessor.process(
			sc.parallelize(uniqueFieldsObjs),
			cc,
			TestConnector.embeddedKeySpace,
			UniquenessIndexTable.tableName(appName),
			UniquenessIndexLookupTable.tableName(appName)
		)

		uniquenessIndexRDD.count() shouldEqual 1
		uniquenessLookupIndexRDD.count() shouldEqual 1

		uniquenessIndexStatusRDD.foreach(_.count() shouldEqual 1)
		def verifyFieldStatus = (fieldStatus: UniqueFieldStatus) => fieldStatus shouldEqual UniqueFieldStatus.pass
		uniquenessIndexStatusRDD.foreach(_.collect.foreach(status => {
			status.username.foreach(verifyFieldStatus)
			status.usernameLookup.foreach(verifyFieldStatus)
		}))
	}

}
