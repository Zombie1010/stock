import sbt.{ExclusionRule, _}

lazy val sparkVersion = "3.2.2"

ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "2.13.10"

ThisBuild / resolvers ++= Seq(
  "Maven Central" at "https://repo1.maven.org/maven2/",
  "Artima Maven Repository" at "https://repo.artima.com/releases",
  "Typesafe repository" at "https://repo.typesafe.com/typesafe/releases/"
)

ThisBuild / versionScheme := Some("early-semver")

//addSbtPlugin("com.artima.supersafe" % "sbtplugin" % "1.1.12")

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion,
  "org.apache.spark" %% "spark-sql" % sparkVersion,
  //  "org.scalamock" %% "scalamock-scalatest-support" % "3.6.0" % Test,
  "org.scalatest" %% "scalatest" % "3.2.9" % Test,
  "org.scalactic" %% "scalactic" % "3.2.9" % Test
)

lazy val root = (project in file("."))
  .settings(
    name := "stock_analysis"
  )