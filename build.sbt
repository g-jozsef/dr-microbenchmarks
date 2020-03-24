import Dependencies._
import scala.io.Source

name := "microbenchmark"

scalaVersion := scalaLanguageVersion

lazy val commonSettings = Seq(
	organizationName := "SZTAKI",
	organization := "hu.sztaki",
	version := {
		val vSource = Source.fromFile("version", "UTF-8")
		val v = vSource.mkString
		vSource.close()
		if (!v.matches("""^[0-9]{1,3}\.[0-9]{1,3}\.[0-9]{1,3}(-SNAPSHOT)?$""")) {
			throw new RuntimeException("Invalid version format!")
		}
		v
	},

	logLevel in test := Level.Debug,
	publishConfiguration := publishConfiguration.value.withOverwrite(true),
	fork in Test := true,
	baseDirectory in Test := (baseDirectory in ThisBuild).value,
	test in assembly := {},
	publishTo := Some("Artifactory Realm" at "https://artifactory.enliven.systems/artifactory/sbt-dev-local/"),
	credentials += Credentials(Path.userHome / ".sbt" / ".credentials"),
	publishArtifact in (Test, packageBin) := true,
)


lazy val core = (project in file("core")).
	settings(commonSettings: _*).
	settings(
		name := "core",
		description := "Core, collection of utilities used in all submodules",
		publishArtifact := false,
		publishArtifact in (Test, packageBin) := false,
		libraryDependencies ++= coreDependencies
	)

lazy val partitioner = (project in file("partitioner")).
	settings(commonSettings: _*).
	settings(
		name := "partitioner",
		description := "Partitioners",
		publishArtifact := false,
		publishArtifact in (Test, packageBin) := false,
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
		publishArtifact := false,
		publishArtifact in (Test, packageBin) := false,
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
	.settings(
		mappings in (Compile, packageBin) ++= {
			val c = (mappings in (core, Compile, packageBin)).value
			val ptt = (mappings in (partitioner, Compile, packageBin)).value
			val bm = (mappings in (benchmark, Compile, packageBin)).value
			c ++ ptt ++ bm
	}
)
