import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.serializer.KryoSerializer

import org.apache.hadoop.fs.Path
import org.apache.hadoop.mapred.{FileOutputFormat, FileOutputCommitter, JobConf}
import org.apache.spark.sql.SQLContext

import org.elasticsearch.hadoop.cfg.ConfigurationOptions
import org.elasticsearch.hadoop.mr.EsOutputFormat

import types.Customer
import types.Order
import helpers.HadoopHelper

import scala.collection.immutable.HashMap


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
    jobConf.set(ConfigurationOptions.ES_RESOURCE, "orders/amount") // index/type
    FileOutputFormat.setOutputPath(jobConf, new Path("-"))

    val sqlContext = new SQLContext(sc)
    // createSchemaRDD is used to implicitly convert an RDD to a SchemaRDD.
    import sqlContext.createSchemaRDD

    // Creating customer table
    val customersFile = sc.textFile(getClass.getResource("customers.csv").toString)
    val customers = customersFile.map(_.split(";")).map(Customer.fromCsv)
    customers.registerAsTable("customers")

    // Creating order table
    val orderFile = sc.textFile(getClass.getResource("orders.csv").toString)
    val orders = orderFile.map(_.split(";")).map(Order.fromCsv)
    orders.registerAsTable("orders")

    // Request
    val req = sqlContext.sql("""
        SELECT c.name, SUM(o.amount)
        FROM customers c JOIN orders o
        ON c.customerId = o.customerId
        GROUP BY c.name""")

    val writables = req.map(rowToMap).map(HadoopHelper.mapToWritable)
    writables.saveAsHadoopDataset(jobConf)
  }

  def rowToMap(row: sql.Row) = {
    val fields = HashMap(
      "name" -> row.getString(0),
      "amount" -> row.getLong(1).toString()
    )
    fields
  }
}
