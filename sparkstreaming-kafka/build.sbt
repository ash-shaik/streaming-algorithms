name := "sparkstreaming-kafka"

version := "0.1"
scalaVersion := "2.12.10"

val sparkVersion = "3.0.2"
val kafkaVersion = "2.4.0"
val log4jVersion = "2.4.1"
val nlpLibVersion = "3.5.1"

resolvers ++= Seq(
  "bintray-spark-packages" at "https://dl.bintray.com/spark-packages/maven",
  "Typesafe Simple Repository" at "https://repo.typesafe.com/typesafe/simple/maven-releases",
  "MavenRepository" at "https://mvnrepository.com"
)

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion,
  "org.apache.spark" %% "spark-sql" % sparkVersion,

  // spark streaming
  "org.apache.spark" %% "spark-streaming" % sparkVersion,
  
  // spark streaming with kafka
  "org.apache.spark" % "spark-sql-kafka-0-10_2.12" % sparkVersion,

  // Dstream integrations
  "org.apache.spark" %% "spark-streaming-kafka-0-10" % sparkVersion,
  

  // logging
  "org.apache.logging.log4j" % "log4j-api" % log4jVersion,
  "org.apache.logging.log4j" % "log4j-core" % log4jVersion,

  // kafka
  "org.apache.kafka" %% "kafka" % kafkaVersion,
  "org.apache.kafka" % "kafka-streams" % kafkaVersion,

  //scala check
  "org.scalacheck" %% "scalacheck" % "1.14.2",

//nlp
  "edu.stanford.nlp" % "stanford-corenlp" % nlpLibVersion,
  "edu.stanford.nlp" % "stanford-corenlp" % nlpLibVersion classifier "models"

)
val circeVersion = "0.14.1"

libraryDependencies ++= Seq(
  "io.circe" %% "circe-core",
  "io.circe" %% "circe-generic",
  "io.circe" %% "circe-parser"
).map(_ % circeVersion)

