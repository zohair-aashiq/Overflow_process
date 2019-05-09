import sbtassembly.PathList

name := "Overflow-processor"

version := "0.1"

scalaVersion := "2.11.12"

libraryDependencies += "org.scala-lang.modules" %% "scala-xml" % "1.1.1"

libraryDependencies += "org.apache.spark" %% "spark-sql" % "2.4.0" 
libraryDependencies += "org.apache.spark" %% "spark-mllib" % "2.4.0"

libraryDependencies += "org.scalactic" %% "scalactic" % "3.0.1"
libraryDependencies += "org.scalatest" %% "scalatest" % "3.0.1" % "test"

libraryDependencies += "com.github.scopt" %% "scopt" % "4.0.0-RC2"

// https://mvnrepository.com/artifact/com.johnsnowlabs.nlp/spark-nlp
libraryDependencies += "com.johnsnowlabs.nlp" %% "spark-nlp" % "1.8.2" exclude("org.tensorflow", "tensorflow") exclude("it.unimi.dsi", "fastutil")  exclude("org.apache.hadoop", "hadoop-aws") exclude ("com.amazonaws", "aws-java-sdk")

// https://mvnrepository.com/artifact/com.typesafe/config
libraryDependencies += "com.typesafe" % "config"  % "1.2.1"

//excludeDependencies += "org.slf4j" % "slf4j-log4j12"
//excludeDependencies += "org.apache.logging.log4j" % "log4j-slf4j-impl"
excludeDependencies += "ch.qos.logback" % "logback-classic"  % "1.2.3"

assemblyMergeStrategy in assembly := {
  case PathList("META-INF", xs @ _*) => MergeStrategy.discard
  case x => MergeStrategy.first
}
