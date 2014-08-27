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

import helpers.HadoopHelper

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
    //val mongoConf = new JobConf(sc.hadoopConfiguration)
    //mongoConf.set("mongo.output.uri", "mongodb://127.0.0.1:27017/abo.output")
    val mongoOrderConf = new JobConf(sc.hadoopConfiguration)
    mongoOrderConf.set("mongo.output.uri", "mongodb://127.0.0.1:27017/abo.order")

    // SparkSQL context
    val sqlContext = new SQLContext(sc)

    // Input file
    val customerPath = "/Users/alex/ab-repo/Faker.js/fake-customer-qn.json"
    val orderPath = "/Users/alex/ab-repo/Faker.js/fake-order-qn.json"
    val orderLinePath = "/Users/alex/ab-repo/Faker.js/fake-orderLine-qn.json"

    // Create a SchemaRDD from the file(s) pointed to by path
    val customer = sqlContext.jsonFile(customerPath)
    val order = sqlContext.jsonFile(orderPath)
    val orderLine = sqlContext.jsonFile(orderLinePath)

    // Check the automatic schema
    customer.printSchema()
    order.printSchema()
    orderLine.printSchema()

    // Register this SchemaRDD as a table.
    customer.registerAsTable("customers")
    order.registerAsTable("orders")
    orderLine.registerAsTable("orderLines")

    // SQL statements can be run by using the sql methods provided by sqlContext.
    val orderDocument = sqlContext.sql("""
      SELECT c.id, c.name, c.siren, c.catchPhrase, o.description, COUNT(*)
      FROM customers c RIGHT JOIN orders o
      ON c.id = o.customerId
      GROUP BY c.id, c.name, c.siren, c.catchPhrase, o.description
      ORDER BY c.name""")


    orderDocument.persist()
    orderLine.persist()

    //println("Count: " + orderDocument.count())
    orderDocument.registerAsTable("orderDocuments")
    //sqlContext.sql("SELECT * FROM orderDocuments o ORDER BY o.name").collect().foreach(println)

    var itemOLD = ""
    var bson = new BasicBSONObject()

    sqlContext.sql("SELECT * FROM orderDocuments o ORDER BY o.name").collect().foreach((row: sql.Row) => {
      var itemNew = row.getString(1)
      if (itemNew == itemOLD) {
        println("order : " + row.getString(4))
        bson.put("order",new BasicBSONObject("orderDescription",row.getString(4)))
        bson.put("order",new BasicBSONObject("orderDescription","toto"))

      }
      else if (itemNew != itemOLD) {
        //var bson = new BasicBSONObject()
        println("customer name : " + row.getString(1))
        bson.put("customerName", row.getString(1))
        println("customer siren : " + row.getString(2))
        bson.put("customerSiren", row.getString(2))
      }
      itemOLD = row.getString(1)
      println("bson : " + bson.toString)

    })


    // To ElasticSearch
    //val writablesES = orderDocument.map(rowToMapES).map(HadoopHelper.mapToWritable)
    //writablesES.saveAsHadoopDataset(esConf)

    // To Mongodb
/*    val orderRDD = orderDocument.map((row: sql.Row) => {
      var bson = new BasicBSONObject()

      bson.put("customerId", row.getInt(0))
      bson.put("customerName", row.getString(1))
      bson.put("customerSiren", row.getString(2))
      bson.put("orderDescription", row.getString(4))
      bson.put("test", "alex")
      (null, bson)
    }) */
    //orderRDD.saveAsNewAPIHadoopFile("file:///bogus", classOf[Any], classOf[Any], classOf[com.mongodb.hadoop.MongoOutputFormat[Any, Any]], mongoOrderConf)

  }

//  def rowToMapES(row: sql.Row) = {
//    val fields = HashMap(
//      "id" -> row.getInt(0).toString(),
//      "nom" -> row.getString(1),
//      "siren" -> row.getString(2),
//      "slogan" -> row.getString(3),
//      "totalOrder" -> row.getLong(4).toString()
//    )
//    fields
//  }

}
