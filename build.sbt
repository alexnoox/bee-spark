name := "bee-idea"

version := "1.0"

scalaVersion := "2.10.4"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "1.1.0",
  "org.apache.spark" %% "spark-sql" % "1.1.0",
  "org.elasticsearch" % "elasticsearch-hadoop" % "2.1.0.Beta1",
  "org.apache.hadoop" % "hadoop-client" % "2.2.0",
  "org.mongodb" % "mongo-hadoop-core" % "1.3.0",
  "org.mongodb" % "mongo-java-driver" % "2.12.2"
)

resolvers ++= Seq(
  "Akka Repository" at "http://repo.akka.io/releases/",
  "Conjars Repository" at "http://conjars.org/repo/"
)

