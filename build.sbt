name := "hadoop-essentials"

version := "0.1"

scalaVersion := "2.13.1"

val hadoopVersion = "2.7.3"

libraryDependencies ++= Seq(
  "org.apache.hadoop" % "hadoop-common",
  "org.apache.hadoop" % "hadoop-hdfs",
).map( _ % hadoopVersion)
