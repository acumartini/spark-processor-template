import Dependencies.Versions
import Settings._
import sbt.Keys.scalacOptions
import sbtassembly.AssemblyPlugin.autoImport.assemblyJarName

resolvers += Resolver.sonatypeRepo("releases")

lazy val root = project.root
	.settings(name := "spark-processor-template")
	.aggregate(
		`common_2_10`,
		`common_2_11`,
		`spark1_6`,
		`spark2`
	)

lazy val common = project.from("common").cross

lazy val `common_2_10` = common(Versions.scala_2_10)
	.settings(
		libraryDependencies ++= Dependencies.idnProcessorCommon,
		Settings.common ++ Seq(
			test in assembly := {}
		)
	)

lazy val `common_2_11` = common(Versions.scala_2_11)
	.settings(
		libraryDependencies ++= Dependencies.idnProcessorCommon,
		Settings.common ++ Seq(
			test in assembly := {}
		)
	)

lazy val `spark1_6` = project.from("spark1_6")
	.dependsOn(`common_2_10` % "test->test;compile->compile")
	.settings(
		name := "spark1_6",
		scalaVersion := Versions.scala_2_10,
		libraryDependencies ++= Dependencies.idnProcessorSpark1_6,
		Settings.common ++ Seq(
			test in assembly := {},
			assemblyJarName in assembly := "spark1_6-shaded.jar"
		)
	)

lazy val `spark2` = project.from("spark2")
	.dependsOn(`common_2_11` % "test->test;compile->compile")
	.settings(
		name := "spark2",
		scalaVersion := Versions.scala_2_11,
		libraryDependencies ++= Dependencies.idnProcessorSpark2,
		Settings.common ++ Seq(
			test in assembly := {},
			assemblyJarName in assembly := "spark2-shaded.jar"
		)
	)

def scalacOptionsForVersion(scalaVersion: String) = CrossVersion.partialVersion(scalaVersion) match {
	case Some(Tuple2(2, 10)) => Settings.compileOptions_2_10
	case Some(Tuple2(2, 11)) => Settings.compileOptions_2_11
	case _ => throw new IllegalStateException(s"Unsupported cross compile version [$scalaVersion]")
}

val appSettings = Seq(
	scalacOptions := scalacOptionsForVersion(scalaVersion.value)
)