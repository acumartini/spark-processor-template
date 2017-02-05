package spark.processor

import com.datastax.spark.connector.cql.CassandraConnector
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import org.apache.spark.SparkConf
import org.apache.spark.sql.SaveMode
import org.joda.time.DateTime
import spark.cassandra.MockTables.{ContactEvents, CoreEvents}
import spark.cassandra.{DBCleanupBeforeAndAfterEach, TestConnector}
import spark.model.Template.{ContactFields, CoreFields, Event, EventPayload}
import spark.model.{RawEvent, UniqueFields}
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

	def genId(i: Int = 0) = s"mock:id:$i"
	def otherId(i: Int = 0) = s"mock:otherId:$i"
	def username(i: Int = 0) = s"mock:username:$i"
	def verifiedPhone(i: Int = 0) = s"mock:verifiedPhone:$i"

	def oldTime: DateTime = DateTime.now().minusDays(random.nextInt(1000) + 1)
	val oldValue = "mock:old:value"

	it should "populate a the missing *optional* unique fields for a collection of UniqueFields objects" in {
		val sqlContext = sqx
		import sqlContext.implicits._ // required for implicit Encoders

		val broadcastMapper = sc.broadcast(mapper)
		val cc = CassandraConnector(sc.getConf)

		Given("Events for ids stored in cassandra")
		val idRange = Range(0, 100)
		val eventRange = Range(0, random.nextInt(5) + 1)

		val uniqueFieldsObjs = idRange.map(i => UniqueFields(genId(i)))

		val coreEvents = idRange.flatMap(i => {
			val oldEvents = eventRange.map(_ => {
				EventPayload[CoreFields](
					id = genId(i),
					time = EventPayload.time(oldTime),
					set = CoreFields(
						Some(genId(i)),
						Some(oldValue),
						Some(oldValue)
					)
				)
			})
			val newEvent = EventPayload[CoreFields](
				id = genId(i),
				set = CoreFields(
					Some(genId(i)),
					Some(otherId(i)),
					Some(username(i))
				)
			)
			oldEvents :+ newEvent
		})
			.map(payload => {
				broadcastMapper.value.registerModule(DefaultScalaModule)
				RawEvent.from(
					payload,
					broadcastMapper.value.writeValueAsString(payload)
				)
			})
			.toDS()

		val contactEvents = idRange.flatMap(i => {
			val oldEvents = eventRange.map(_ => {
				EventPayload[ContactFields](
					id = genId(i),
					time = EventPayload.time(oldTime),
					set = ContactFields(
						Some(oldValue)
					)
				)
			})
			val newEvent = EventPayload[ContactFields](
				id = genId(i),
				set = ContactFields(
					Some(verifiedPhone(i))
				)
			)
			oldEvents :+ newEvent
		})
			.map(payload => {
				broadcastMapper.value.registerModule(DefaultScalaModule)
				RawEvent.from(
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

		Given("UniqueFields objects with only a id")
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
				val id = result.id.substring(result.id.lastIndexOf(":") + 1).toInt
				result.id shouldEqual genId(id)
				result.otherId shouldEqual Some(otherId(id))
				result.username shouldEqual Some(username(id))
				result.verifiedPhone shouldEqual Some(verifiedPhone(id))
			})
	}

	it should "properly handle $rem statements" in {
		Given("Events with latest record remove statement")
		val coreEvents = List(
			Event.from(
				EventPayload[CoreFields](
					id = genId(),
					time = EventPayload.time(oldTime),
					set = CoreFields(
						Some(genId()),
						Some(otherId()),
						Some(username())
					)
				)
			),
			Event.from(
				EventPayload[CoreFields](
					id = genId(),
					set = CoreFields(),
					rem = List("otherId", "username")
				)
			)
		)
		val uniqueFieldsEventsRdd = sc.parallelize(Seq(
			UniqueFieldsEvents(
				UniqueFields(genId()),
				coreEvents
			)
		))

		When("UniqueFields are populated")
		UniqueFieldsPopulationProcessor.process(uniqueFieldsEventsRdd)
			.collect()
			.foreach(result => {
				result.id shouldEqual genId()
				result.otherId shouldBe None
				result.username shouldBe None
				result.verifiedPhone shouldBe None
			})
	}

}
