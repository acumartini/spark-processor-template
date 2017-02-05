package spark

import org.apache.spark.sql.SparkSession

/**
	*
	* @author Adam Martini
	*/
trait SparkSqlContext extends AbstractSparkTest {

	@transient private var _sqx: SparkSession = _
	def sqx: SparkSession = _sqx

	override def beforeAll() {
		super.beforeAll()

		_sqx = SparkSession
			.builder()
			.getOrCreate()
	}

}
