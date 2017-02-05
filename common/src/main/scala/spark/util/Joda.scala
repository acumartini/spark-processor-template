package spark.util

import org.joda.time.DateTime

/**
	*
	* @author Adam Martini
	*/
object Joda {
	implicit def dateTimeOrdering: Ordering[DateTime] = Ordering.fromLessThan(_ isBefore _)
}
