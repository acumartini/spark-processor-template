package spark.cassandra

import org.scalatest.BeforeAndAfterEach


/**
	*
	* @author Adam Martini
	*/
trait DBCleanupAfterEach extends EmbeddedCassandra
	with BeforeAndAfterEach {

	override def afterEach(): Unit = {
		truncateTables()
		super.afterEach()
	}

}