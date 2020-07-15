name := "BonialAssessment"

version := "0.1"

scalaVersion := "2.11.11"

// https://mvnrepository.com/artifact/org.apache.spark/spark-core
libraryDependencies += "org.apache.spark" %% "spark-core" % "2.3.3"

// https://mvnrepository.com/artifact/org.apache.spark/spark-sql
libraryDependencies += "org.apache.spark" %% "spark-sql" % "2.3.3"

// https://mvnrepository.com/artifact/org.apache.spark/spark-streaming
libraryDependencies += "org.apache.spark" %% "spark-streaming" % "2.3.3" % "provided"

// https://mvnrepository.com/artifact/org.apache.spark/spark-hive
libraryDependencies += "org.apache.spark" %% "spark-hive" % "2.3.3" % "provided"


// https://mvnrepository.com/artifact/org.apache.spark/spark-hive
libraryDependencies += "org.apache.spark" %% "spark-hive" % "2.3.3" % "provided"

// https://mvnrepository.com/artifact/org.apache.kafka/kafka-streams
libraryDependencies += "org.apache.kafka" % "kafka-streams" % "2.3.0"

libraryDependencies += "org.apache.kafka" %% "kafka-streams-scala" % "2.3.0"

// https://mvnrepository.com/artifact/org.apache.kafka/kafka-clients
libraryDependencies += "org.apache.kafka" % "kafka-clients" % "2.3.0"

// https://mvnrepository.com/artifact/org.apache.kafka/kafka
libraryDependencies += "org.apache.kafka" %% "kafka" % "2.3.0"
fork in Test := true
javaOptions ++= Seq("-Xms512M", "-Xmx2048M", "-XX:MaxPermSize=2048M", "-XX:+CMSClassUnloadingEnabled")
parallelExecution in Test := false

libraryDependencies += "com.holdenkarau" %% "spark-testing-base" % "2.3.3_0.12.0" 

// https://mvnrepository.com/artifact/com.typesafe/config
libraryDependencies += "com.typesafe" % "config" % "1.3.4"

// https://mvnrepository.com/artifact/org.vegas-viz/vegas-spark
libraryDependencies += "org.vegas-viz" %% "vegas-spark" % "0.3.11"

libraryDependencies += "org.json4s" %% "json4s-jackson" % "3.3.0"

libraryDependencies += "org.json4s" %% "json4s-native" % "3.3.0"

// https://mvnrepository.com/artifact/org.scalatest/scalatest

libraryDependencies += "org.scalatest" %% "scalatest" % "2.2.2"
libraryDependencies += "org.scalatest" %% "scalatest" % "2.2.2" % Test

//libraryDependencies += "com.datastax.cassandra" % "cassandra-driver-core" % "3.0.0"

libraryDependencies += "com.datastax.cassandra" % "cassandra-driver-core" % "3.9.0"

resolvers += "Spark Packages Repo" at "http://dl.bintray.com/spark-packages/maven"
// Scala 2.11
libraryDependencies += "MrPowers" % "spark-fast-tests" % "0.17.1-s_2.11"

// https://mvnrepository.com/artifact/org.apache.hive/hive-jdbc
libraryDependencies += "org.apache.hive" % "hive-jdbc" % "2.3.2"

// https://mvnrepository.com/artifact/com.hortonworks.hive/hive-warehouse-connector
//libraryDependencies += "com.hortonworks.hive" %% "hive-warehouse-connector" % "1.0.0.7.1.1.0-565"


