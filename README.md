# Bee Spark

Project to map input data files, reduce to a given format and export to elasticsearch.

Package this project using `$ sbt package`

Launch with `YOUR_SPARK_HOME/bin/spark-submit --class Main target/scala-2.10/bee-spark-project_2.10-1.0.jar`

### Increase Mac OS max file limit
add `limit maxfiles 16384 32768` to /etc/launchd.conf (create it if not exist) and then reboot