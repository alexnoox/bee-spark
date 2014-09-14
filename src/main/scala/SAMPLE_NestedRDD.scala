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
    case class Order (orderId: Int, customerId: Int, orderName: String)
    case class OrderLine (orderLineId: Int, orderId: Int, lineAmount: Int )

    val customer = sc.textFile(getClass.getResource("fake-customer-qn.csv").toString).map(_.split(";")).map(
      c => (c(0).toInt, Customer(c(0).toInt, c(1) ))
    )
    val order = sc.textFile(getClass.getResource("fake-order-qn.csv").toString).map(_.split(";")).map(
      o => (o(0).toInt, Order(o(0).toInt, o(1).toInt, o(2) ))
    )
    val orderLine = sc.textFile(getClass.getResource("fake-orderLine-qn.csv").toString).map(_.split(";")).map(
      ol => (ol(1).toInt, OrderLine(ol(0).toInt, ol(1).toInt, ol(3).toInt))
    )

    case class mainCustomer (customerId: Int, name: String, order: mainOrder)
    case class mainOrder (orderId: Int, customerId: Int, orderName: String, orderLine: mainOrderLine)
    case class mainOrderLine (orderLineId: Int, orderId: Int, lineAmount: Int )


    val x = List("a" -> "b", "c" -> "d", "a" -> "f")
    //x: List[(java.lang.String, java.lang.String)] = List((a,b), (c,d), (a,f))
    val x1 = x.groupBy(_._1)
    val x2 = x.groupBy(_._1).map { case (k,v) => (k,v.map(_._2))}
    println("--------")
    x1.foreach(println)
    println("--------")
    x2.foreach(println)

    println("--------")
    /*val tuple_o_ol = order.cogroup(orderLine).map(x => (x._2._1.head.customerId, (
      x._2._1.head.orderId,
      x._2._1.head.customerId,
      x._2._1.head.orderName,
      x._2._2.toList
    )))*/

    val tuple_o_ol = order.cogroup(orderLine).map { case (k,v) => (v._1.head.customerId,(v._1,v._2)) }
    tuple_o_ol.foreach(println)

    println("--------")
    val tuple = customer.cogroup(tuple_o_ol)
    //val tuple = customer.cogroup(order.cogroup(orderLine))
    tuple.foreach(println)



    val r1 = tuple.map((t) => {
      val custBson = new BasicBSONObject()
      val orderBsonList = new BasicBSONList()
      custBson.put("customerId", t._2._1.head.customerId)
      custBson.put("name", t._2._1.head.name)

      t._2._2.foreach { o =>
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
