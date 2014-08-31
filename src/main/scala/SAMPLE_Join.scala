import org.apache.hadoop.fs.Path
import org.apache.hadoop.mapred.{FileOutputCommitter, FileOutputFormat, JobConf}
import org.apache.spark.SparkContext._
import org.apache.spark._
import org.apache.spark.serializer.KryoSerializer
import org.apache.spark.sql.SQLContext
import org.bson.BasicBSONObject
import org.bson.types.BasicBSONList

import org.elasticsearch.hadoop.cfg.ConfigurationOptions
import org.elasticsearch.hadoop.mr.EsOutputFormat

import scala.collection.immutable.HashMap

import types.Customer

import helpers.HadoopHelper

object SAMPLE_Join {
  def main(args: Array[String]) {
    // Spark Context setup
    val conf = new SparkConf().setMaster("local").setAppName("Bee-Spark")
    val sc = new SparkContext(conf)
    //sc.setLocalProperty("spark.serializer", classOf[KryoSerializer].getName)

    val mongoCustomerConf = new JobConf(sc.hadoopConfiguration)
    mongoCustomerConf.set("mongo.output.uri", "mongodb://127.0.0.1:27017/abo.customer")

    val format = new java.text.SimpleDateFormat("yyyy-MM-dd")

    case class Customer (customerId: Int, name: String, catchPhrase: String, siren: String)
    case class Order (orderId: Int, customerId: Int, orderName: String, d1: java.util.Date, d2: java.util.Date, d3: java.util.Date)


    //val customersFile = sc.textFile(getClass.getResource("fake-customer-qn.csv").toString)

    val customer = sc.textFile(getClass.getResource("fake-customer-qn.csv").toString).map(_.split(";")).map(
      r => (r(0), Customer(r(0).toInt, r(1), r(2), r(3)))
    )

    val order = sc.textFile(getClass.getResource("fake-order-qn.csv").toString).map(_.split(";")).map(
      c => (c(1), Order(c(0).toInt, c(1).toInt, c(2), null, null, null))
    )

    //customer.join(order).foreach(println)

    customer.cogroup(order).foreach(println)

    println("count : " + customer.cogroup(order).count())

    val customerRDD = customer.cogroup(order).map((tuple) => {
      var bson = new BasicBSONObject()
      bson.put("customerId", tuple._2.)
      (null, bson)
    })

    customerRDD.saveAsNewAPIHadoopFile("file:///bogus", classOf[Any], classOf[Any], classOf[com.mongodb.hadoop.MongoOutputFormat[Any, Any]], mongoCustomerConf)





    //val lines = sc.textFile("/Users/alex/ab-repo/Faker.js/fake-customer-qn.json")
    //val count = lines.flatMap(l => l.split(";"))

/*    val customer = sc.parallelize(List(
      (1, "alex", "c1", "12345"),
      (2, "fred", "c2", "12345"),
      (3, "tomy", "c3", "12345"),
      (4, "alex", "c4", "12345")
    ))

    val order = sc.parallelize(List(
      (1, 1, "alex", 500),
      (2, 1, "fred", 300),
      (3, 1, "tomy", 200),
      (4, 2, "yann", 250)
    ))

    customer.join(order).foreach(println)
    println("--> " + customer.distinct().count())*/

  }

}
