name := "bee-idea"

version := "1.0"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "1.0.1",
  "org.elasticsearch" % "elasticsearch-hadoop" % "2.0.0"
)

resolvers ++= Seq(
  "Akka Repository" at "http://repo.akka.io/releases/",
  "Conjars Repository" at "http://conjars.org/repo/"
)