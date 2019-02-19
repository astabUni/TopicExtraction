name := "topicExtraction"

version := "0.1"

scalaVersion := "2.11.12"

scalacOptions += "-target:jvm-1.8"

libraryDependencies ++= {

  Seq(
    "edu.stanford.nlp" % "stanford-corenlp" % "3.9.2",
    "edu.stanford.nlp" % "stanford-corenlp" % "3.9.2" classifier "models",
    //"com.google.protobuf" % "protobuf-java" % "3.2.0",
    // "org.apache.spark" %% "spark-mllib-local" % "2.4.0",
    "org.apache.spark" %% "spark-core" % "2.4.0",
    "org.apache.spark" %% "spark-sql" % "2.4.0",
    "org.apache.spark" %% "spark-mllib" % "2.4.0",
    "com.databricks" %% "spark-xml" % "0.5.0",
    "com.github.master" % "spark-stemming_2.10" % "0.2.1"
  )
}

dependencyOverrides += "com.fasterxml.jackson.core" % "jackson-databind" % "2.6.7.1"