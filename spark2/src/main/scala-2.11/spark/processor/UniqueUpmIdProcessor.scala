package spark.processor

import org.apache.spark.rdd.RDD
import spark.model.UniqueFields

/**
	* Accepts a RDD of UniqueFields and map-reduces them across all available executors to return a list of tuples where
	* tuple_1 represents a id and tuple_2 represents the count for the number of times the id is found in the
	* UniqueFields RDD data set, such that each id in the tuple list is unique.
	*/
object  UniqueIdProcessor {

	def process(data: RDD[UniqueFields]): RDD[(String, Int)] = {
		data
			.map(data => (data.id, 1))
			.reduceByKey(_ + _)
	}

}