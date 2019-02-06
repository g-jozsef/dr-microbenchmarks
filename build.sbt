name := "dr-microbenchmarks"

version := "0.1"

scalaVersion := "2.12.8"

lazy val root = (project in file(".")).
	settings(
		libraryDependencies += "org.scalatest" % "scalatest_2.11" % "3.0.1" % "test",
		libraryDependencies += "org.scalacheck" %% "scalacheck" % "1.13.4" % "test",
		libraryDependencies += "com.google.guava" % "guava" % "19.0"
	)