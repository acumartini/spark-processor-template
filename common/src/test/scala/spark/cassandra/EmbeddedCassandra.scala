package spark.cassandra

import com.datastax.driver.core.{Cluster, Session}
import org.cassandraunit.utils.EmbeddedCassandraServerHelper
import org.scalatest.BeforeAndAfterAll
import spark.UnitSpec

import scala.util.control.Exception


/**
	*
	* @author Adam Martini
	*/
trait EmbeddedCassandra extends UnitSpec
	with BeforeAndAfterAll {

	lazy val cluster: Cluster = TestConnector.embedded
	implicit lazy val session: Session = cluster.connect()

	// Creates all tables required for test scope
	def createTables(): Unit

	// Truncates all tables required for test scope
	def truncateTables(): Unit

	override def beforeAll(): Unit = {
		super.beforeAll()
		// attempt to start embedded cassandra, ignoring errors related to pre-existing instance
		Exception.ignoring(classOf[Throwable]) {
			EmbeddedCassandraServerHelper.startEmbeddedCassandra("embedded-cassandra.yaml", 20000L)
		}
		session.execute(
			s"CREATE KEYSPACE IF NOT EXISTS ${TestConnector.embeddedKeySpace} WITH replication = {'class': 'SimpleStrategy', 'replication_factor' : 1};"
		)
		createTables()
	}

	override def afterAll() {
		truncateTables()
		session.close()
		cluster.close()
		super.afterAll()
	}

}