package spark.cassandra

/**
	*
	* @author Adam Martini
	*/
trait CassandraDB {
	protected def keyspace(ksp: String): (String, String) = "keyspace" -> ksp
	protected def table(table: String): (String, String) = "table" -> table
}
trait CassandraKeyspace extends CassandraDB {
	def keyspace: String
}
trait CassandraTable extends CassandraKeyspace {
	def table: String
	def keyspaceTableMap: Map[String, String]
}

object TemplateDB extends CassandraDB {

	object BioKeyspace extends CassandraKeyspace {
		val keyspace = "bio"
	}
	object BioTable extends CassandraTable {
		val keyspace: String = BioKeyspace.keyspace
		val table = "event"
		val keyspaceTableMap = Map(keyspace(keyspace), table(table))
	}

	object ContactKeyspace extends CassandraKeyspace {
		val keyspace = "contact"
	}
	object ContactTable extends CassandraTable {
		val keyspace: String = ContactKeyspace.keyspace
		val table = "event"
		val keyspaceTableMap = Map(keyspace(keyspace), table(table))
	}

	object ContextKeyspace extends CassandraKeyspace {
		val keyspace = "context"
	}
	object ContextTable extends CassandraTable {
		val keyspace: String = ContextKeyspace.keyspace
		val table = "event"
		val keyspaceTableMap = Map(keyspace(keyspace), table(table))
	}

	object CoreKeyspace extends CassandraKeyspace {
		val keyspace = "core"
	}
	object CoreTable extends CassandraTable {
		val keyspace: String = CoreKeyspace.keyspace
		val table = "event"
		val keyspaceTableMap = Map(keyspace(keyspace), table(table))
	}

	object SettingsKeyspace extends CassandraKeyspace {
		val keyspace = "settings"
	}
	object SettingsTable extends CassandraTable {
		val keyspace: String = SettingsKeyspace.keyspace
		val table = "event"
		val keyspaceTableMap = Map(keyspace(keyspace), table(table))
	}

	object SubscriptionsKeyspace extends CassandraKeyspace {
		val keyspace = "subscriptions"
	}
	object SubscriptionsTable extends CassandraTable {
		val keyspace: String = SubscriptionsKeyspace.keyspace
		val table = "event"
		val keyspaceTableMap = Map(keyspace(keyspace), table(table))
	}

	object IdnCacheKeyspace extends CassandraKeyspace {
		val keyspace = "cache"
	}
	object OldUniquenessIndex extends CassandraTable {
		val keyspace: String = IdnCacheKeyspace.keyspace
		val table = "unique_index"
		val keyspaceTableMap = Map(keyspace(keyspace), table(table))
	}
	object OldUniquenessLookupIndex extends CassandraTable {
		val keyspace: String = IdnCacheKeyspace.keyspace
		val table = "unique_index_lookup"
		val keyspaceTableMap = Map(keyspace(keyspace), table(table))
	}

	object UniquenessKeyspace extends CassandraKeyspace {
		val keyspace = "test"
	}
	object UniquenessIndexTable extends CassandraTable {
		val keyspace: String = UniquenessKeyspace.keyspace
		val table = "unique_index"
		val keyspaceTableMap = Map(keyspace(keyspace), table(table))
	}
	object UniquenessLookupIndexTable extends CassandraTable {
		val keyspace: String = UniquenessKeyspace.keyspace
		val table = "unique_index_lookup"
		val keyspaceTableMap = Map(keyspace(keyspace), table(table))
	}

}
