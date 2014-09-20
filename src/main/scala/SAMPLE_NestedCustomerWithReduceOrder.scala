import org.apache.hadoop.mapred.JobConf
import org.apache.spark.SparkContext._
import org.apache.spark._
import org.apache.spark.rdd.RDD
import org.bson.BasicBSONObject
import org.bson.types.BasicBSONList

object SAMPLE_NestedCustomerWithReduceOrder {
  def main(args: Array[String]) {

    // Spark Context setup
    val conf = new SparkConf().setMaster("local").setAppName("Bee-Spark")
    val sc = new SparkContext(conf)

    val mongoCustomerConf = new JobConf(sc.hadoopConfiguration)
    mongoCustomerConf.set("mongo.output.uri", "mongodb://127.0.0.1:27017/abo.customer")

    val format = new java.text.SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss")

    case class CustomerIn (customerId: Int, name: String, isGoldCustomer: Boolean)
    case class ContactIn (contactId: Int, customerId: Int, firstName: String,  lastName: String, email: String, tel: String, avatar: String)
    case class Order (orderId: Int, customerId: Int, orderName: String, total: Double, average: Double, max: Double, isGoodOrder: Boolean )
    case class OrderLine (orderLineId: Int, orderId: Int, amount: Double, max: Double, sum: Int){}
    //case class OrderLineStat (orderId: Int, lineAmout: Double, sum: Int, maxLineAmout: Double, isGoodLine: Boolean){}

    println("----customer----")
    val customer = sc.textFile(getClass.getResource("fake-customer-qn.csv").toString).map(_.split(";")).map(
        c => (c(0).toInt, CustomerIn(c(0).toInt, c(1), false ))
    )
    customer.foreach(println)

    println("----contact----")
    val contact = sc.textFile(getClass.getResource("fake-contact-qn.csv").toString).map(_.split(";")).map(
        c => (c(1).toInt, ContactIn(c(0).toInt, c(1).toInt, c(2), c(3), c(4), c(5), c(6) ))
    )
    contact.foreach(println)

    println("----order----")
    var order = sc.textFile(getClass.getResource("fake-order-qn.csv").toString).map(_.split(";")).map(
        o => (o(0).toInt, Order(o(0).toInt, o(1).toInt, o(2), 0, 0, 0, false))
    )
    order.foreach(println)

    println("----orderLine----")
    var orderLine = sc.textFile(getClass.getResource("fake-orderLine-qn.csv").toString).map(_.split(";")).map(
        ol => (ol(1).toInt, OrderLine(ol(0).toInt, ol(1).toInt, ol(3).toDouble, 0, 1))
    )
    orderLine.foreach(println)

    println("----orderLine----")
    orderLine = orderLine
      .reduceByKey((a, b) =>
      OrderLine(
        a.orderLineId, //orderLineId
        a.orderId, //orderId
        a.amount + b.amount, //amout
        a.amount + b.amount / 10, //max
        a.sum + b.sum //orderline number
      ))
    orderLine.sortByKey().foreach(println)

    println("----order----")
    order = order.join(orderLine)
      .map(x => (
      x._2._1.customerId,
        Order(
          x._2._1.orderId, //orderId
          x._2._1.customerId, //customerId
          x._2._1.orderName, //name
          x._2._2.amount, //total
          x._2._2.amount/x._2._2.sum,
          x._2._2.max,//if (a.amount > b.amount) a.amount else b.amount,
          if (x._2._2.amount > 100) true else false //isGoodDeal
        )))
    order.sortByKey().foreach(println)

    println("--------")
    val tuple = customer.cogroup(order).cogroup(contact)

    val r1 = tuple.map((t) => {
      val custBson = new BasicBSONObject()
      val orderBsonList = new BasicBSONList()
      val customerBsonList = new BasicBSONList()

      custBson.put("customerId", t._2._1.head._1.head.customerId)
      custBson.put("name", t._2._1.head._1.head.name)

      t._2._2.foreach { (c: ContactIn) =>
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

      t._2._1.head._2.foreach { o =>
        val orderBson = new BasicBSONObject()
        val orderLineBsonList = new BasicBSONList()
        orderBson.put("id", o.orderId)
        orderBson.put("customerId", o.customerId)
        orderBson.put("orderDescription", o.orderName)
        orderBson.put("orderTotal", o.total)
        orderBson.put("avergae", o.average)
        orderBson.put("max", o.max)
        orderBson.put("isGoodDeal", o.isGoodOrder)

        /*var total = 0.0
        o._2.foreach { (ol: OrderLine) =>
          val orderLineBson = new BasicBSONObject()
          orderLineBson.put("id", ol.orderLineId)
          orderLineBson.put("orderId", ol.orderId)
          orderLineBson.put("amout", ol.lineAmount)
          total = total + ol.lineAmount
          orderLineBsonList.add(orderLineBson)
        }*/

        //orderBson.put("orderAmout", total)
        //orderBson.put("orderlines", orderLineBsonList)
        orderBsonList.add(orderBson)

      }
      custBson.put("orders", orderBsonList)

      (null, custBson)
    })

    r1.saveAsNewAPIHadoopFile("file:///bogus", classOf[Any], classOf[Any], classOf[com.mongodb.hadoop.MongoOutputFormat[Any, Any]], mongoCustomerConf)

  }

  def average(numbers: RDD[Int]): Int = {
    val(sum, count) = numbers.map(n => (n, 1)).reduce{(a, b) => (a._1 + b._1, a._2 + b._2)}
    sum/count
  }

}
