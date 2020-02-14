import sbt.Keys.libraryDependencies
import sbt._

object Dependencies {
  lazy val coreDependencies: Seq[ModuleID] = Seq(
    "org.scala-lang" % "scala-library" % "2.12.10",
    "org.scala-lang" % "scala-compiler" % "2.12.10",
    "org.scalatest" %% "scalatest" % "3.0.5" % Test,
    // https://mvnrepository.com/artifact/org.json4s/json4s-native
    "org.json4s" %% "json4s-native" % "3.6.7",
    // https://mvnrepository.com/artifact/org.tukaani/xz
    "org.tukaani" % "xz" % "1.8"
  )

  lazy val partitionerDependenices: Seq[ModuleID] = Seq(
    "com.google.guava" % "guava" % "19.0"
  )

  lazy val benchmarkDependencies: Seq[ModuleID] = Seq()

  lazy val vizualizationDependencies: Seq[ModuleID] = Seq()
}