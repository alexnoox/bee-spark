import org.apache.hadoop.mapred.JobConf
import org.apache.spark.SparkContext._
import org.apache.spark._
import org.bson.BasicBSONObject
import org.bson.types.BasicBSONList

object SAMPLE_NestedRDD {
  def main(args: Array[String]) {
    // Spark Context setup
    val conf = new SparkConf().setMaster("local").setAppName("Bee-Spark")
    val sc = new SparkContext(conf)
    //sc.setLocalProperty("spark.serializer", classOf[KryoSerializer].getName)

    val mongoCustomerConf = new JobConf(sc.hadoopConfiguration)
    mongoCustomerConf.set("mongo.output.uri", "mongodb://127.0.0.1:27017/abo.customer")

    val format = new java.text.SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss")

    case class Customer (customerId: Int, name: String)
    case class Contact (contactId: Int, customerId: Int, firstName: String,  lastName: String, email: String, tel: String, avatar: String)
    case class Order (orderId: Int, customerId: Int, orderName: String)
    case class OrderLine (orderLineId: Int, orderId: Int, lineAmount: Int )

    val customer = sc.textFile(getClass.getResource("fake-customer-qn.csv").toString).map(_.split(";")).map(
      c => (c(0).toInt, Customer(c(0).toInt, c(1) ))
    )
    val contact = sc.textFile(getClass.getResource("fake-contact-qn.csv").toString).map(_.split(";")).map(
      c => (c(1).toInt, Contact(c(0).toInt, c(1).toInt, c(2), c(3), c(4), c(5), c(6) ))
    )
    val order = sc.textFile(getClass.getResource("fake-order-qn.csv").toString).map(_.split(";")).map(
      o => (o(0).toInt, Order(o(0).toInt, o(1).toInt, o(2) ))
    )
    val orderLine = sc.textFile(getClass.getResource("fake-orderLine-qn.csv").toString).map(_.split(";")).map(
      ol => (ol(1).toInt, OrderLine(ol(0).toInt, ol(1).toInt, ol(3).toInt))
    )


    /*
    val x = List("a" -> "b", "c" -> "d", "a" -> "f")
    val x1 = x.groupBy(_._1)
    //(a,List((a,b), (a,f)))
    //(c,List((c,d)))
    val x2 = x.groupBy(_._1).map { case (k,v) => (k,v.map(_._2))}
    //(a,List(b, f))
    //(c,List(d))
    */


    println("--------")
    val tuple = customer.cogroup(
      order.cogroup(orderLine).map { case (k,v) => (v._1.head.customerId,(v._1,v._2)) }
    ).cogroup(contact)

    val r1 = tuple.map((t) => {
      val custBson = new BasicBSONObject()
      val orderBsonList = new BasicBSONList()
      val customerBsonList = new BasicBSONList()

      custBson.put("customerId", t._2._1.head._1.head.customerId)
      custBson.put("name", t._2._1.head._1.head.name)

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
        orderBson.put("id", o._1.head.orderId)
        orderBson.put("customerId", o._1.head.customerId)
        orderBson.put("orderDescription", o._1.head.orderName)

        o._2.foreach { (ol: OrderLine) =>
          val orderLineBson = new BasicBSONObject()
          orderLineBson.put("id", ol.orderLineId)
          orderLineBson.put("orderId", ol.orderId)
          orderLineBson.put("amout", ol.lineAmount)
          orderLineBsonList.add(orderLineBson)
        }
        orderBson.put("orderlines", orderLineBsonList)
        orderBsonList.add(orderBson)
      }
      custBson.put("orders", orderBsonList)

      (null, custBson)
    })

    r1.saveAsNewAPIHadoopFile("file:///bogus", classOf[Any], classOf[Any], classOf[com.mongodb.hadoop.MongoOutputFormat[Any, Any]], mongoCustomerConf)

  }

}
