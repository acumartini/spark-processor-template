package spark

import org.apache.spark.sql.SQLContext

/**
	*
	* @author Adam Martini
	*/
trait SparkSqlContext extends AbstractSparkTest {

	@transient private var _sqx: SQLContext = _
	def sqx: SQLContext = _sqx

	override def beforeAll() {
		super.beforeAll()

		_sqx = new SQLContext(sc)
	}

}
