import helpers.HadoopHelper
import org.apache.hadoop.fs.Path
import org.apache.hadoop.mapred.{FileOutputCommitter, FileOutputFormat, JobConf}
import org.apache.spark.SparkContext._
import org.apache.spark._
import org.apache.spark.serializer.KryoSerializer
import org.apache.spark.sql.SQLContext
import org.elasticsearch.hadoop.cfg.ConfigurationOptions
import org.elasticsearch.hadoop.mr.EsOutputFormat

import scala.collection.immutable.HashMap


object CustomerService {
  def main(args: Array[String]) {
    // Spark Context setup
    val conf = new SparkConf().setMaster("local").setAppName("Bee-Spark")
    val sc = new SparkContext(conf)
    sc.setLocalProperty("spark.serializer", classOf[KryoSerializer].getName)

    // Elasticsearch-Hadoop setup
    val jobConf = new JobConf(sc.hadoopConfiguration)
    jobConf.setOutputFormat(classOf[EsOutputFormat])
    jobConf.setOutputCommitter(classOf[FileOutputCommitter])
    jobConf.set(ConfigurationOptions.ES_NODES, "localhost")
    jobConf.set(ConfigurationOptions.ES_PORT, "9200")
    jobConf.set(ConfigurationOptions.ES_RESOURCE, "customer/sample") // index/type
    FileOutputFormat.setOutputPath(jobConf, new Path("-"))

    val sqlContext = new SQLContext(sc)

    val customerPath = "/Users/alex/ab-repo/Faker.js/fake-customer-fta.json"
    val orderPath = "/Users/alex/ab-repo/Faker.js/fake-order-fta.json"

    // Create a SchemaRDD from the file(s) pointed to by path
    val customer = sqlContext.jsonFile(customerPath)
    val order = sqlContext.jsonFile(orderPath)
    customer.printSchema()
    order.printSchema()

    // Register this SchemaRDD as a table.
    customer.registerAsTable("customers")
    order.registerAsTable("orders")

    // SQL statements can be run by using the sql methods provided by sqlContext.
    val customerView = sqlContext.sql("""
      SELECT c.id, c.name, c.siren, c.catchPhrase, COUNT(*)
      FROM customers c JOIN orders o
      ON c.id = o.customerId
      GROUP BY c.id, c.name, c.siren, c.catchPhrase""")

/*    val req = sqlContext.sql(
        SELECT c.name, SUM(o.amount)
        FROM customers c JOIN orders o
        ON c.customerId = o.customerId
        GROUP BY c.name)*/


    customerView.persist()

    customerView.map(t => s"result: $t").collect().foreach(println)
    println("Count: " + customerView.count())

    // To ElasticSearch
    val writables = customerView.map(rowToMap).map(HadoopHelper.mapToWritable)
    writables.saveAsHadoopDataset(jobConf)

  }

  def rowToMap(row: sql.Row) = {
    val fields = HashMap(
      "id" -> row.getInt(0).toString,
      "nom" -> row.getString(1),
      "siren" -> row.getString(2),
      "slogan" -> row.getString(3),
      "totalOrder" -> row.getLong(4).toString()
    )
    fields
  }
}
