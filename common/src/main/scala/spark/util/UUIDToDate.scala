package spark.util

import java.util.UUID

import org.joda.time.DateTime

/**
	*
	* @author Adam Martini
	*/
object UUIDToDate {

	private val NUM_100NS_INTERVALS_SINCE_UUID_EPOCH = 0x01b21dd213814000L

	def time(uuid: UUID): Long = (uuid.timestamp() - NUM_100NS_INTERVALS_SINCE_UUID_EPOCH) / 10000
	def dateTime(uuid: UUID): DateTime = new DateTime(time(uuid))

}
