name := "bee-idea"

version := "1.0"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "1.0.2",
  "org.apache.spark" %% "spark-sql" % "1.0.2",
  "org.elasticsearch" % "elasticsearch-hadoop" % "2.0.0",
  "org.apache.hadoop" % "hadoop-client" % "2.2.0",
  "org.mongodb" % "mongo-hadoop-core" % "1.3.0",
  "org.mongodb" % "mongo-java-driver" % "2.11.4"
)

resolvers ++= Seq(
  "Akka Repository" at "http://repo.akka.io/releases/",
  "Conjars Repository" at "http://conjars.org/repo/"
)