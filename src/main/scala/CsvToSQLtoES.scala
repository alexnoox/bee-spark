import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.serializer.KryoSerializer

import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.{MapWritable, NullWritable, Text}
import org.apache.hadoop.mapred.{FileOutputFormat, FileOutputCommitter, JobConf}
import org.apache.spark.sql.SQLContext

import org.elasticsearch.hadoop.cfg.ConfigurationOptions
import org.elasticsearch.hadoop.mr.EsOutputFormat
import types.Customer

object CsvToSQLtoES {
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

    val sqlContext = new SQLContext(sc)
    // createSchemaRDD is used to implicitly convert an RDD to a SchemaRDD.
    import sqlContext.createSchemaRDD

    // Creating customer table
    val csvFile = sc.textFile(getClass.getResource("customers.csv").toString)
    val customers = csvFile.map(_.split(";")).map(Customer.fromCsv)
    customers.registerAsTable("customers")

    // Request
    val teenagers = sqlContext.sql("SELECT name FROM customers WHERE customerId >= 13 AND customerId <= 19")
    teenagers.map(t => "Name: " + t(0)).collect().foreach(println)
  }
}
