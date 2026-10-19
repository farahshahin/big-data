name := "docsim"

version := "1.0"

scalaVersion := "2.12.18"

libraryDependencies += "org.apache.spark" %% "spark-core" % "3.5.3"

Compile / run / fork := true

Compile / run / javaOptions += "--add-exports=java.base/sun.nio.ch=ALL-UNNAMED"