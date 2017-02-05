package spark

import com.typesafe.config.{Config, ConfigFactory}
import org.scalatest._

abstract class UnitSpec extends FlatSpec
	with Matchers
	with GivenWhenThen
	with OptionValues
	with Inside
	with Inspectors {

	val config: Config = ConfigFactory.load("processor-test")

}
