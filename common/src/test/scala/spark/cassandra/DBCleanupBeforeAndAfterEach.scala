package spark.cassandra

import org.scalatest.BeforeAndAfterEach


/**
	*
	* @author Adam Martini
	*/
trait DBCleanupBeforeAndAfterEach extends EmbeddedCassandra
	with BeforeAndAfterEach {

	override def beforeEach(): Unit = {
		super.beforeEach()
		truncateTables()
	}

	override def afterEach(): Unit = {
		truncateTables()
		super.afterEach()
	}

}