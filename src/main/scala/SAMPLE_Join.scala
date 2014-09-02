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
    val mongoOrderConf = new JobConf(sc.hadoopConfiguration)
    mongoCustomerConf.set("mongo.output.uri", "mongodb://127.0.0.1:27017/abo.customer")
    mongoOrderConf.set("mongo.output.uri", "mongodb://127.0.0.1:27017/abo.order")

    val format = new java.text.SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss")

    case class Customer (customerId: Int, name: String, catchPhrase: String, siren: String)
    case class Order (orderId: Int, customerId: Int, orderName: String, createdDate: java.util.Date, updatedDate: java.util.Date, shippedDate: java.util.Date)
    case class Contact (contactId: Int, customerId: Int, firstName: String,  lastName: String, email: String, tel: String, avatar: String)
    case class OrderLine (orderLineId: Int, orderId: Int, name: String, desc: String, cat: String, quantity: Int, unitPrice: Float, lineAmount: Int )

    val customer = sc.textFile(getClass.getResource("fake-customer-qn.csv").toString).map(_.split(";")).map(
      c => (c(0), Customer(c(0).toInt, c(1), c(2), c(3) ))
    )
    val orderC = sc.textFile(getClass.getResource("fake-order-qn.csv").toString).map(_.split(";")).map(
      o => (o(1), Order(o(0).toInt, o(1).toInt, o(2), format.parse(o(3)), format.parse(o(4)), format.parse(o(5) )))
    )
    val contact = sc.textFile(getClass.getResource("fake-contact-qn.csv").toString).map(_.split(";")).map(
      c => (c(1), Contact(c(0).toInt, c(1).toInt, c(2), c(3), c(4), c(5), c(6) ))
    )
    val orderO = sc.textFile(getClass.getResource("fake-order-qn.csv").toString).map(_.split(";")).map(
      o => (o(0), Order(o(0).toInt, o(1).toInt, o(2), format.parse(o(3)), format.parse(o(4)), format.parse(o(5) )))
    )
    val orderLine = sc.textFile(getClass.getResource("fake-orderLine-qn.csv").toString).map(_.split(";")).map(
      ol => (ol(1), OrderLine(ol(0).toInt, ol(1).toInt, ol(2), ol(3), ol(4), ol(5).toInt, ol(6).toFloat, ol(7).toInt))
    )

    val orderRDD = orderO.cogroup(orderLine).map((tuple) => {
      val orderBson = new BasicBSONObject()
      val orderLineBsonList = new BasicBSONList()

      orderBson.put("id", tuple._2._1.head.orderId)
      orderBson.put("customerId", tuple._2._1.head.customerId)
      orderBson.put("orderDescription", tuple._2._1.head.orderName)
      orderBson.put("createDate", tuple._2._1.head.createdDate.toString)
      orderBson.put("updateDate", tuple._2._1.head.updatedDate.toString)
      orderBson.put("shipDate", tuple._2._1.head.shippedDate.toString)

      var orderAmount = 0
      tuple._2._2.foreach { (ol: OrderLine) =>
        val orderLineBson = new BasicBSONObject()
        orderLineBson.put("id", ol.orderLineId)
        orderLineBson.put("orderId", ol.orderId)
        orderLineBson.put("name", ol.name)
        orderLineBson.put("description", ol.desc)
        orderLineBson.put("category", ol.cat)
        orderLineBson.put("quantity", ol.quantity)
        orderLineBson.put("unitPrice", ol.unitPrice)
        orderLineBson.put("amount", ol.lineAmount)
        orderAmount = orderAmount + ol.lineAmount
        orderLineBsonList.add(orderLineBson)
      }
      orderBson.put("orderAmout", orderAmount)
      orderBson.put("orderLines", orderLineBsonList)

      (null, orderBson)
    })

    orderRDD.saveAsNewAPIHadoopFile("file:///bogus", classOf[Any], classOf[Any], classOf[com.mongodb.hadoop.MongoOutputFormat[Any, Any]], mongoOrderConf)

    val customerRDD = customer.cogroup(orderC, contact).map((tuple) => {
      val custBson = new BasicBSONObject()
      val orderBsonList = new BasicBSONList()
      val customerBsonList = new BasicBSONList()

      custBson.put("id", tuple._2._1.head.customerId)
      custBson.put("name", tuple._2._1.head.name)
      custBson.put("catchPhrase", tuple._2._1.head.catchPhrase)
      custBson.put("siren", tuple._2._1.head.siren)

      tuple._2._3.foreach { (c: Contact) =>
        val contactBson = new BasicBSONObject()
        contactBson.put("id", c.contactId)
        contactBson.put("customerId", c.customerId)
        contactBson.put("firstName", c.firstName)
        contactBson.put("lastName", c.lastName)
        contactBson.put("email", c.email)
        contactBson.put("phone", c.tel)
        contactBson.put("avatar", c.avatar)
        customerBsonList.add(contactBson)
      }
      custBson.put("contacts", customerBsonList)

      tuple._2._2.foreach { (o: Order) =>
        val orderBson = new BasicBSONObject()
        orderBson.put("id", o.orderId)
        orderBson.put("customerId", o.customerId)
        orderBson.put("orderDescription", o.orderName)
        orderBson.put("createDate", o.createdDate.toString)
        orderBson.put("updateDate", o.updatedDate.toString)
        orderBson.put("shipDate", o.shippedDate.toString)
        orderBsonList.add(orderBson)
      }
      custBson.put("orders", orderBsonList)

      (null, custBson)
    })

    customerRDD.saveAsNewAPIHadoopFile("file:///bogus", classOf[Any], classOf[Any], classOf[com.mongodb.hadoop.MongoOutputFormat[Any, Any]], mongoCustomerConf)
  }

}
