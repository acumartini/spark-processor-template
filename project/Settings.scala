import sbt.Keys._
import sbt.{Project, Test, file}

object Settings {

	val common = Seq(
		version := "1.0.0-SNAPSHOT",
		crossScalaVersions := Seq("2.10.6", "2.11.8"),
		organization := "template",
		parallelExecution in Test := false,
		javaOptions ++= Seq("-Xms512M", "-Xmx2048M", "-XX:MaxPermSize=2048M", "-XX:+CMSClassUnloadingEnabled")
	)

	val compileOptions_2_10 = Seq(
		"-target:jvm-1.7",
		"-encoding", "UTF-8",
		"-unchecked",
		"-deprecation",
		"-feature",
		"-language:existentials",
		"-language:higherKinds",
		"-language:implicitConversions",
		"-language:postfixOps",
		"-Yno-adapted-args",
		"-Ywarn-dead-code",
		"-Xfatal-warnings",
		"-Xlint"
	)

	val compileOptions_2_11 = Seq(
		"-target:jvm-1.8",
		"-encoding", "UTF-8",
		"-unchecked",
		"-deprecation",
		"-feature",
		"-language:existentials",
		"-language:higherKinds",
		"-language:implicitConversions",
		"-language:postfixOps",
		"-Yno-adapted-args",
		"-Ywarn-dead-code",
		"-Ywarn-infer-any",
		"-Ywarn-unused-import",
		"-Xfatal-warnings",
		"-Xlint"
	)

	implicit class ProjectRoot(project: Project) {
		def root: Project = project in file(".")
	}

	implicit class ProjectFrom(project: Project) {
		def from(dir: String): Project = project in file(dir)
	}

}
