import sbt._

object Dependencies {

	//// Versions

	object Versions {
		val scala_2_10 = "2.10.6"
		val scala_2_11 = "2.11.8"
	}


	//// Bundles

	// cassandra
	private val cassandraCore = Seq(
		"com.datastax.cassandra" % "cassandra-driver-core" % "3.1.2" % "provided",
		"org.apache.cassandra" % "cassandra-all" % "3.9" excludeAll(
			ExclusionRule("commons-logging", "commons-logging"),
			ExclusionRule("org.slf4j", "jcl-over-slf4j"),
			ExclusionRule("org.slf4j", "log4j-over-slf4j"),
			ExclusionRule("ch.qos.logback", "logback-classic")
		)
	)

	private val cassandraTest = Seq(
		"org.cassandraunit" % "cassandra-unit" % "3.1.1.0" % "test"
	)

	// aws
	private val aws = Seq(
		"com.amazonaws" % "aws-java-sdk" % "1.10.77" % "provided"
	)

	// spark 1.6
	private val spark1_6 = Seq(
		"org.apache.spark" % "spark-core_2.10" % "1.6.1" % "provided" exclude("org.scalatest", "scalatest_2.11"),
		"org.apache.spark" % "spark-sql_2.10" % "1.6.1" % "provided",
		"com.datastax.spark" % "spark-cassandra-connector_2.10" % "1.5.1" % "provided",
		"com.fasterxml.jackson.core" % "jackson-core" % "2.7.4",
		"com.fasterxml.jackson.module" % "jackson-module-scala_2.10" % "2.7.4",
		"com.holdenkarau" % "spark-testing-base_2.10" % "1.6.1_0.4.7" % "test"
	)

	// spark 2
	private val spark2 = Seq(
		"org.apache.spark" % "spark-core_2.11" % "2.0.2" % "provided" excludeAll(
			ExclusionRule("org.scalatest", "scalatest_2.11"),
			ExclusionRule("org.apache.hadoop", "hadoop-auth")
		),
		"org.apache.spark" % "spark-streaming_2.11" % "2.0.2" % "provided" excludeAll(
			ExclusionRule("org.scalatest", "scalatest_2.11"),
			ExclusionRule("org.apache.hadoop", "hadoop-auth")
		),
		"org.apache.spark" % "spark-sql_2.11" % "2.0.2" % "provided",
		"com.databricks" % "spark-avro_2.11" % "3.1.0" % "provided",
		"org.apache.spark" % "spark-sql_2.11" % "2.0.2" % "provided",
		"com.databricks" % "spark-avro_2.11" % "3.1.0" % "provided",
		"com.datastax.spark" % "spark-cassandra-connector_2.11" % "2.0.0-M3" % "provided",
		"org.apache.hadoop" % "hadoop-aws" % "3.0.0-alpha1" % "provided",
		"org.apache.hadoop" % "hadoop-auth" % "3.0.0-alpha1" % "provided",
		"com.holdenkarau" % "spark-testing-base_2.11" % "2.0.2_0.4.7" % "test",
		"org.elasticmq" % "elasticmq-server_2.11" % "0.12.1"
	)

	// utils
	private val utils = Seq(
		"com.typesafe" % "config" % "1.3.1",
		"org.rogach" %% "scallop" % "2.0.5",
		"org.scalactic" %% "scalactic" % "3.0.1",
		"joda-time" % "joda-time" % "2.9.6",
		"org.joda" % "joda-convert" % "1.8.1",
		"com.fasterxml.jackson.module" %% "jackson-module-scala" % "2.8.4"
	)

	// test
	private val test = Seq(
		"org.scalatest" %% "scalatest" % "3.0.1" % "test"
	)


	//// Modules

	val processorCommon: Seq[ModuleID] = aws ++ cassandraCore ++ cassandraTest ++ utils ++ test
	val processorSpark1_6: Seq[ModuleID] = spark1_6 ++ aws ++ utils ++ test ++ cassandraTest
	val processorSpark2: Seq[ModuleID] = spark2 ++ aws ++ utils ++ test ++ cassandraTest

}
