import org.apache.hadoop.fs.Path
import org.apache.hadoop.mapred.{FileOutputCommitter, FileOutputFormat, JobConf}
import org.apache.spark.SparkContext._
import org.apache.spark._
import org.apache.spark.serializer.KryoSerializer
import org.apache.spark.sql.SQLContext
import org.bson.BasicBSONObject
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
    val esConf = new JobConf(sc.hadoopConfiguration)
    esConf.setOutputFormat(classOf[EsOutputFormat])
    esConf.setOutputCommitter(classOf[FileOutputCommitter])
    esConf.set(ConfigurationOptions.ES_NODES, "localhost")
    esConf.set(ConfigurationOptions.ES_PORT, "9200")
    esConf.set(ConfigurationOptions.ES_RESOURCE, "customer/sample") // index/type
    FileOutputFormat.setOutputPath(esConf, new Path("-"))

    // Mongo setup
    val mongoConf = new JobConf(sc.hadoopConfiguration)
    mongoConf.set("mongo.output.uri", "mongodb://127.0.0.1:27017/abo.output")


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

    customerView.registerAsTable("customerViews")
    println("Result of SELECT * customerViews table :")

    sqlContext.sql("SELECT * FROM customerViews").collect().foreach(println)

    customerView.persist()


    customer.map(t => s"result: $t").collect().foreach(println)

    println("Count: " + customerView.count())

    // To ElasticSearch
    //val writablesES = customerView.map(rowToMapES).map(HadoopHelper.mapToWritable)
    //writablesES.saveAsHadoopDataset(esConf)

    val saveRDD = customerView.map((row: sql.Row) => {
      var bson = new BasicBSONObject()
      bson.put("nom", row.getString(1))
      bson.put("siren", row.getString(2))
      bson.put("slogan", row.getString(3))
      (null, bson)
    })
    saveRDD.saveAsNewAPIHadoopFile("file:///bogus", classOf[Any], classOf[Any], classOf[com.mongodb.hadoop.MongoOutputFormat[Any, Any]], mongoConf)
  }

  def rowToMapES(row: sql.Row) = {
    val fields = HashMap(
      "id" -> row.getInt(0).toString(),
      "nom" -> row.getString(1),
      "siren" -> row.getString(2),
      "slogan" -> row.getString(3),
      "totalOrder" -> row.getLong(4).toString()
    )
    fields
  }

}
