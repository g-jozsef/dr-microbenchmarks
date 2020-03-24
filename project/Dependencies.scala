import sbt.Keys.libraryDependencies
import sbt._

object Dependencies {
  val scalaLanguageVersion = "2.12.11"

  lazy val coreDependencies: Seq[ModuleID] = Seq(
    "org.scala-lang" % "scala-library" % scalaLanguageVersion
  )

  lazy val partitionerDependenices: Seq[ModuleID] = Seq(
    "com.google.guava" % "guava" % "19.0"
  )

  lazy val benchmarkDependencies: Seq[ModuleID] = Seq(
    "com.google.guava" % "guava" % "19.0"
  )

  lazy val vizualizationDependencies: Seq[ModuleID] = Seq()
}