name := "SQL Streaming"

version := "1.0"

scalaVersion := "2.9.3"

libraryDependencies += "org.apache.spark" %% "spark-core" % "0.9.0-incubating-SNAPSHOT"

libraryDependencies += "org.apache.spark" %% "spark-streaming" % "0.9.0-incubating-SNAPSHOT"

resolvers += "Akka Repository" at "http://repo.akka.io/releases/"
