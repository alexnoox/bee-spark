import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.serializer.KryoSerializer

import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.{MapWritable, NullWritable, Text}
import org.apache.hadoop.mapred.{FileOutputFormat, FileOutputCommitter, JobConf}

import org.elasticsearch.hadoop.cfg.ConfigurationOptions
import org.elasticsearch.hadoop.mr.EsOutputFormat
import types.Car

import scala.collection.immutable.HashMap

object WriteCsvToES {
  def main(args: Array[String]) {
    // Spark Context setup
    val conf = new SparkConf().setMaster("local").setAppName("Bee-Spark")
    val sc = new SparkContext(conf)
    sc.setLocalProperty("spark.serializer", classOf[KryoSerializer].getName)

    // Elasticsearch-Hadoop setup
    val jobConf = new JobConf(sc.hadoopConfiguration)
    jobConf.setOutputFormat(classOf[EsOutputFormat])
    jobConf.setOutputCommitter(classOf[FileOutputCommitter])
    jobConf.set(ConfigurationOptions.ES_NODES, "vps67962.ovh.net")
    jobConf.set(ConfigurationOptions.ES_PORT, "9200")
    jobConf.set(ConfigurationOptions.ES_RESOURCE, "cars/car") // index/type
    FileOutputFormat.setOutputPath(jobConf, new Path("-"))

    // Reading a CSV file
    val csvFile = sc.textFile(getClass.getResource("cars.csv").toString)
    val cars = csvFile.map(_.split(";")).map(Car.fromCsv)

    // Counting price sum per model
    val carsPrice = cars.map(car => (car.model, car.price))
    val modelSum = carsPrice.reduceByKey(_ + _)
    modelSum.collect().foreach(println)

    // Counting total price sum
    val secondColumn = cars.map(_.price)
    val sum = secondColumn.max
    println(s"Sum of cars prices: $sum")

    // Writing RDD to ElasticSearch
    val writables = cars.map(Car.toMap).map(mapToOutput)
    writables.saveAsHadoopDataset(jobConf)
  }

  def mapToOutput(in: Map[String, String]): (Object, Object) = {
    val m = new MapWritable
    for ((k, v) <- in)
      m.put(new Text(k), new Text(v))
    (NullWritable.get, m)
  }
}
