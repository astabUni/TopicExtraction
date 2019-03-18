
name := "topicExtraction"

version := "0.1"

scalaVersion := "2.11.12"

scalacOptions += "-target:jvm-1.8"


libraryDependencies ++= {

  Seq(
    "edu.stanford.nlp" % "stanford-corenlp" % "3.9.2",
    "edu.stanford.nlp" % "stanford-corenlp" % "3.9.2" classifier "models",
    "com.google.protobuf" % "protobuf-java" % "3.2.0",
    "org.apache.spark" %% "spark-core" % "2.4.0",
    "org.apache.spark" %% "spark-sql" % "2.4.0",
    "org.apache.spark" %% "spark-mllib" % "2.4.0"

  )
}

