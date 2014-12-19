# Bee Spark

Project to map input data files, reduce to a given format and export to elasticsearch.

Package this project using `$ sbt package`

Launch with `YOUR_SPARK_HOME/bin/spark-submit --class Main target/scala-2.10/bee-spark-project_2.10-1.0.jar`

### Increase Mac OS max file limit (mongodb connection)
add `limit maxfiles 16384 32768` to /etc/launchd.conf (create it if not exist) and then reboot

### Launch bee-spark as a standalone server
`sudo ../spark-1.2.0-bin-hadoop2.4/bin/spark-submit --jars /Users/alex/bee-spark/lib/mongo-java-driver-2.12.2.jar,/Users/alex/bee-spark/lib/mongo-hadoop-core-1.3.0.jar,/Users/alex/bee-spark/lib/elasticsearch-hadoop-2.1.0.Beta1.jar --class "SAMPLE_NestedCustomerWithReduceOrder" --master local[4] target/scala-2.10/bee-idea_2.10-1.0.jar`