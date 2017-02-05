package spark.cassandra

import com.datastax.spark.connector._
import spark.SparkSqlContext
import org.apache.spark.SparkConf
import spark.AbstractSparkTest

class CassandraConnectorTest1_6 extends AbstractSparkTest
	with EmbeddedCassandra
	with SparkSqlContext {

	val appName = "CassandraConnectorTest1_6"

	override val conf: SparkConf = sparkConf
		.set("spark.cassandra.connection.host", TestConnector.embeddedHost)
		.set("spark.cassandra.connection.port", TestConnector.embeddedPort.toString)

	override def createTables(): Unit = {
		KeyValue.createTable(appName)
		User.createTable(appName)
	}

	override def truncateTables(): Unit = {
		KeyValue.truncateTable(appName)
		User.truncateTable(appName)
	}

	behavior of "CassandraConnector"

	it should "should store and retrieve simple RDD data " in {

		val collection = sc.parallelize(Seq(("key3", 3), ("key4", 4)))
		collection.saveToCassandra(
			TestConnector.embeddedKeySpace,
			KeyValue.tableName(appName),
			SomeColumns("key", "value")
		)

		val rdd = sc.cassandraTable(TestConnector.embeddedKeySpace, KeyValue.tableName(appName))

		rdd.count shouldEqual 2
		rdd.map(_.getInt("value")).sum shouldEqual 7
	}

	it should "should store and retrieve data as Dataset objects through sqlContext" in {
		val sqlContext = sqx
		import sqlContext.implicits._ // required for implicit Encoders

		val users = Seq(
			User(firstName = "mock1:firstName", lastName = "mock1:lastName"),
			User(firstName = "mock2:firstName", lastName = "mock2:lastName")
		).toDF()

		users.write
			.format("org.apache.spark.sql.cassandra")
			.options(User.keyspaceTableMap(appName))
			.save()

		val dataset = sqlContext
			.read
			.format("org.apache.spark.sql.cassandra")
			.options(User.keyspaceTableMap(appName))
			.load()
			.as[User]

		dataset.count shouldEqual 2
		dataset.filter(user => user.firstName == "mock1:firstName").count() shouldEqual 1
		dataset.filter(user => user.firstName == "mock2:firstName").count() shouldEqual 1
	}

}