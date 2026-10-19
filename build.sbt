
name := "docsim"

version := "1.0"

scalaVersion := "2.13.18"

libraryDependencies +="org.apache.spark" %% "spark-core" % "4.2.0"

Compile / run / fork := true
