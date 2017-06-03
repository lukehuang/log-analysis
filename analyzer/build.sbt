name := "analyzer"

version := "1.0"

scalaVersion := "2.11.11"

resolvers ++= Seq(
  "Sonatype OSS Snapshots" at "https://oss.sonatype.org/content/repositories/snapshots",
  "Apache Staging" at "https://repository.apache.org/content/repositories/releases/",
  "Apache Snapshots" at "https://repository.apache.org/content/repositories/snapshots/",
  "jitpack" at "https://jitpack.io"
)

libraryDependencies ++= Seq(
  "com.typesafe" % "config" % "1.3.1",
  "org.apache.kafka" %% "kafka" % "0.10.2.1",
  "org.apache.spark" %% "spark-core" % "2.1.0",
  "org.apache.spark" %% "spark-streaming" % "2.1.0",
  "org.apache.spark" %% "spark-streaming-kafka-0-8" % "2.1.0"
)
