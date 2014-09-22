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

    case class Contact (contactId: Int,
                        customerId: Int,
                        firstName: String,
                        lastName: String,
                        email: String,
                        tel: String,
                        avatar: String)

    case class Customer (customerId: Int,
                         name: String,
                         total: Double,
                         numberOfOrder: Int,
                         avg: Double,
                         max: Double)

    case class CustomerIn (customerId: Int,
                           name: String)

    case class OrderStat (orderId: Int,
                          customerId: Int,
                          orderName: String,
                          total: Double,
                          numberOfOrder: Int,
                          avg: Double,
                          max: Double)

    case class Order (customerId: Int,
                      orderId: Int,
                      orderName: String,
                      total: Double,
                      numberOfLine: Int)

    case class OrderIn (orderId: Int,
                        customerId: Int,
                        orderName: String,
                        numberOfOrder: Int)

    case class OrderLineStat (orderId: Int,
                              total: Double,
                              numberOfLine: Int)

    println("----customer----")
    val customerIn = sc.textFile(getClass.getResource("fake-customer-qn.csv").toString).map(_.split(";")).map(
        c => (c(0).toInt, CustomerIn(c(0).toInt, c(1) ))
    )
    customerIn.foreach(println)

    println("----contact----")
    val contact = sc.textFile(getClass.getResource("fake-contact-qn.csv").toString).map(_.split(";")).map(
        c => (c(1).toInt, Contact(c(0).toInt, c(1).toInt, c(2), c(3), c(4), c(5), c(6) ))
    )
    contact.foreach(println)

    println("----order----")
    var orderIn = sc.textFile(getClass.getResource("fake-order-qn.csv").toString).map(_.split(";")).map(
        o => (o(0).toInt, OrderIn(o(0).toInt, o(1).toInt, o(2), 1))
    )
    orderIn.foreach(println)


    println("----orderLineStat----")
    val order = sc.textFile(getClass.getResource("fake-orderLine-qn.csv").toString).map(_.split(";")).map(ol => (ol(1).toInt, (ol(0).toInt, ol(1).toInt, ol(3).toDouble, 1)) )
    .reduceByKey({ case ((a1, b1, c1, d1), (a2, b2, c2, d2)) => (a1, b1, c1 + c2, d1 + d2) })
    .map({ case (k, (i1, i2, sum, count) ) => (k, OrderLineStat(i2, sum, count) ) })
    .join(orderIn)
    .map({ case (k,v) => (v._2.customerId, Order(v._2.customerId, v._2.orderId, v._2.orderName, v._1.total, v._1.numberOfLine))})

    order.sortByKey().foreach(println)


    println("----orderStat----")
    val customer = order.map( {case (k,v) => (v.customerId, (v.orderId, v.customerId, v.orderName, v.total, v.numberOfLine, v.total))} )
      .reduceByKey({ case ((a1, b1, c1, d1, e1,f1), (a2, b2, c2, d2, e2, f2)) => (a1, b1, c1, d1+d2, e1+e2, if (f1 > f2) f1 else f2 ) })
      .map({ case (k, (i1, i2, name, sum, count, max) ) => (i2, OrderStat(i1, i2, name, sum, count, sum/count, max) ) })
      .join(customerIn)
      .map({ case (k,v) => (v._2.customerId, Customer(v._2.customerId, v._2.name, v._1.total, v._1.numberOfOrder, v._1.avg, v._1.max))})
    customer.sortByKey().foreach(println)


    println("--------")
    val tuple = customer.cogroup(order.map({case (k,v) => (v.customerId, v)})).cogroup(contact)

    val r1 = tuple.map((t) => {
      val custBson = new BasicBSONObject()
      val orderBsonList = new BasicBSONList()
      val customerBsonList = new BasicBSONList()

      custBson.put("customerId", t._2._1.head._1.head.customerId)
      custBson.put("name", t._2._1.head._1.head.name)
      custBson.put("total", t._2._1.head._1.head.total)
      custBson.put("max", t._2._1.head._1.head.max)
      custBson.put("avg", t._2._1.head._1.head.avg)

      t._2._2.foreach { (c: Contact) =>
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
