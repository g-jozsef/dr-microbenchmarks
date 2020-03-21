import Dependencies._
import scala.io.Source

name := "dr-microbenchmarks"

scalaVersion := scalaLanguageVersion

lazy val commonSettings = Seq(
	version := {
		val vSource = Source.fromFile("version", "UTF-8")
		val v = vSource.mkString
		vSource.close()
		if (!v.matches("""^[0-9]{1,3}\.[0-9]{1,3}\.[0-9]{1,3}$""")) {
			throw new RuntimeException("Invalid version format!")
		}
		v
	},

	logLevel in test := Level.Debug,

	fork in Test := true,
	baseDirectory in Test := (baseDirectory in ThisBuild).value,
	test in assembly := {},
)


lazy val core = (project in file("core")).
	settings(commonSettings: _*).
	settings(
		name := "core",
		description := "Core, collection of utilities used in all submodules",
		libraryDependencies ++= coreDependencies
	)

lazy val partitioner = (project in file("partitioner")).
	settings(commonSettings: _*).
	settings(
		name := "partitioner",
		description := "Partitioners",
		libraryDependencies ++= partitionerDependenices
	).
	dependsOn(
		core % "test->test;compile->compile"
	)

lazy val benchmark = (project in file("benchmark")).
	settings(commonSettings: _*).
	settings(
		name := "benchmark",
		description := "Benchmark, benchmarking and visualizing partitioners",
		libraryDependencies ++= benchmarkDependencies
	).
	dependsOn(
		core % "test->test;compile->compile",
		partitioner % "test->test;compile->compile"
	)

lazy val microbenchmarks = (project in file(".")).
	settings(commonSettings: _*).
	aggregate(core, partitioner, benchmark).
	dependsOn(
		core % "test->test;compile->compile",
		partitioner % "test->test;compile->compile",
		benchmark % "test->test;compile->compile"
	)
