package spark

import com.holdenkarau.spark.testing.{RDDComparisons, SharedSparkContext}
import org.apache.spark.SparkConf

/**
	*
	* @author Adam Martini
	*/
abstract class AbstractSparkTest extends UnitSpec
	with SharedSparkContext
	with RDDComparisons {

	def appName: String

	def sparkConf: SparkConf = new SparkConf()
		.setMaster("local[*]")
		.setAppName(s"${appName}Test")
		.set("spark.ui.enabled", "false")
		.set("spark.app.id", appID)
		.set("spark.driver.allowMultipleContexts", "true")

}
