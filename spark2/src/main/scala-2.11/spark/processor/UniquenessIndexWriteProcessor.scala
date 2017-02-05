package spark.processor

import org.apache.spark.sql.{Dataset, SaveMode}
import spark.model.Template.{UniquenessIndex, UniquenessIndexLookup}

/**
	* Accepts Datasets of UniquenessIndex and UniquenessIndexLookup, and writes them to the given tables without
	* overwriting existing data.
	*/
object UniquenessIndexWriteProcessor {

	def process(
		uniquenessIndexDS: Dataset[UniquenessIndex],
		uniquenessIndexTableMap: Map[String, String],
		uniquenessIndexLookupDS: Dataset[UniquenessIndexLookup],
		uniquenessIndexLookupTableMap: Map[String, String]
	): Unit = {
		uniquenessIndexDS.write
			.format("org.apache.spark.sql.cassandra")
			.options(uniquenessIndexTableMap)
			.mode(SaveMode.Append)
			.save()

		uniquenessIndexLookupDS.write
			.format("org.apache.spark.sql.cassandra")
			.options(uniquenessIndexLookupTableMap)
			.mode(SaveMode.Append)
			.save()
	}

}
