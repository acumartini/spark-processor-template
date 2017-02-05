package spark.cassandra

import com.datastax.driver.core.{Cluster, QueryOptions}

/**
	*
	* @author Adam Martini
	*/
object TestConnector {

	val embeddedHost = "localhost"
	val embeddedPort = 9142
	val embeddedKeySpace = "test"

	def cluster(host: String, port: Int): Cluster = new Cluster.Builder()
		.addContactPoints(host)
		.withPort(port)
		.withQueryOptions(new QueryOptions())
		.build

	def embedded: Cluster = cluster(embeddedHost, embeddedPort)

}
