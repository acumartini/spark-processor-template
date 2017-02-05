package spark.processor

import com.datastax.spark.connector.cql.CassandraConnector
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import org.apache.spark.SparkConf
import org.apache.spark.sql.SaveMode
import org.joda.time.DateTime
import spark.cassandra.MockTables.{ContactEvents, CoreEvents}
import spark.cassandra.{DBCleanupBeforeAndAfterEach, TestConnector}
import spark.model.Identity.{ContactFields, CoreFields, IdentityEvent, IdentityPayload}
import spark.model.{RawIdentityEvent, UniqueFields}
import spark.{AbstractSparkTest, SparkSqlContext}

import scala.util.Random

/**
	*
	* @author Adam Martini
	*/
class UniqueFieldsPopulationProcessorTest extends AbstractSparkTest
	with DBCleanupBeforeAndAfterEach
	with SparkSqlContext {

	val appName = "UniqueFieldsPopulationProcessor"

	override val conf: SparkConf = sparkConf
		.set("spark.cassandra.connection.host", TestConnector.embeddedHost)
		.set("spark.cassandra.connection.port", TestConnector.embeddedPort.toString)

	val mapper = new ObjectMapper()
	val datetime = new DateTime()
	val random = new Random()

	override def createTables(): Unit = {
		CoreEvents.createTable(appName)
		ContactEvents.createTable(appName)
	}

	override def truncateTables(): Unit = {
		CoreEvents.truncateTable(appName)
		ContactEvents.truncateTable(appName)
	}

	behavior of appName

	def upmId(i: Int = 0) = s"mock:upmId:$i"
	def nuId(i: Int = 0) = s"mock:nuId:$i"
	def username(i: Int = 0) = s"mock:username:$i"
	def verifiedPhone(i: Int = 0) = s"mock:verifiedPhone:$i"

	def oldTime: DateTime = DateTime.now().minusDays(random.nextInt(1000) + 1)
	val oldValue = "mock:old:value"

	it should "populate a the missing *optional* unique fields for a collection of UniqueFields objects" in {
		val sqlContext = sqx
		import sqlContext.implicits._ // required for implicit Encoders

		val broadcastMapper = sc.broadcast(mapper)
		val cc = CassandraConnector(sc.getConf)

		Given("Events for upmIds stored in cassandra")
		val idRange = Range(0, 100)
		val eventRange = Range(0, random.nextInt(5) + 1)

		val uniqueFieldsObjs = idRange.map(i => UniqueFields(upmId(i)))

		val coreEvents = idRange.flatMap(i => {
			val oldEvents = eventRange.map(_ => {
				IdentityPayload[CoreFields](
					id = upmId(i),
					time = IdentityPayload.time(oldTime),
					set = CoreFields(
						Some(upmId(i)),
						Some(oldValue),
						Some(oldValue)
					)
				)
			})
			val newEvent = IdentityPayload[CoreFields](
				id = upmId(i),
				set = CoreFields(
					Some(upmId(i)),
					Some(nuId(i)),
					Some(username(i))
				)
			)
			oldEvents :+ newEvent
		})
			.map(payload => {
				broadcastMapper.value.registerModule(DefaultScalaModule)
				RawIdentityEvent.from(
					payload,
					broadcastMapper.value.writeValueAsString(payload)
				)
			})
			.toDS()

		val contactEvents = idRange.flatMap(i => {
			val oldEvents = eventRange.map(_ => {
				IdentityPayload[ContactFields](
					id = upmId(i),
					time = IdentityPayload.time(oldTime),
					set = ContactFields(
						Some(oldValue)
					)
				)
			})
			val newEvent = IdentityPayload[ContactFields](
				id = upmId(i),
				set = ContactFields(
					Some(verifiedPhone(i))
				)
			)
			oldEvents :+ newEvent
		})
			.map(payload => {
				broadcastMapper.value.registerModule(DefaultScalaModule)
				RawIdentityEvent.from(
					payload,
					broadcastMapper.value.writeValueAsString(payload)
				)
			})
			.toDS()

		coreEvents.write
			.format("org.apache.spark.sql.cassandra")
			.options(CoreEvents.keyspaceTableMap(appName))
			.mode(SaveMode.Append)
			.save()
		contactEvents.write
			.format("org.apache.spark.sql.cassandra")
			.options(ContactEvents.keyspaceTableMap(appName))
			.mode(SaveMode.Append)
			.save()

		Given("UniqueFields objects with only a upmId")
		val uniqueFieldsRdd = sc.parallelize(uniqueFieldsObjs)

		Given("Events extracted from Cassandra for each UniqueFields object")
		val coreEventsTable = CoreEvents.tableName(appName)
		val contactEventsTable = ContactEvents.tableName(appName)
		val uniqueFieldsEventsRdd = UniqueFieldsPopulationProcessor.extractEvents(
			cc,
			TestConnector.embeddedKeySpace,
			coreEventsTable,
			TestConnector.embeddedKeySpace,
			contactEventsTable,
			uniqueFieldsRdd
		)

		When("Missing UniqueFields properties are populated")
		UniqueFieldsPopulationProcessor.process(uniqueFieldsEventsRdd)
			.collect()
			.foreach(result => {
				val id = result.upmId.substring(result.upmId.lastIndexOf(":") + 1).toInt
				result.upmId shouldEqual upmId(id)
				result.nuId shouldEqual Some(nuId(id))
				result.username shouldEqual Some(username(id))
				result.verifiedPhone shouldEqual Some(verifiedPhone(id))
			})
	}

	it should "properly handle $rem statements" in {
		Given("Events with latest record remove statement")
		val coreEvents = List(
			IdentityEvent.from(
				IdentityPayload[CoreFields](
					id = upmId(),
					time = IdentityPayload.time(oldTime),
					set = CoreFields(
						Some(upmId()),
						Some(nuId()),
						Some(username())
					)
				)
			),
			IdentityEvent.from(
				IdentityPayload[CoreFields](
					id = upmId(),
					set = CoreFields(),
					rem = List("nuId", "username")
				)
			)
		)
		val uniqueFieldsEventsRdd = sc.parallelize(Seq(
			UniqueFieldsEvents(
				UniqueFields(upmId()),
				coreEvents
			)
		))

		When("UniqueFields are populated")
		UniqueFieldsPopulationProcessor.process(uniqueFieldsEventsRdd)
			.collect()
			.foreach(result => {
				result.upmId shouldEqual upmId()
				result.nuId shouldBe None
				result.username shouldBe None
				result.verifiedPhone shouldBe None
			})
	}

}
