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
    case class OrderLine (orderLineId: Int, orderId: Int, orderName: String, lineAmount: Int )

    val customer = sc.textFile(getClass.getResource("fake-customer-qn.csv").toString).map(_.split(";")).map(
      c => (c(0).toInt, Customer(c(0).toInt, c(1) ))
    )
    val order = sc.textFile(getClass.getResource("fake-order-qn.csv").toString).map(_.split(";")).map(
      o => (o(0).toInt, Order(o(0).toInt, o(1).toInt, o(2) ))
    )
    val orderLine = sc.textFile(getClass.getResource("fake-orderLine-qn.csv").toString).map(_.split(";")).map(
      ol => (ol(1).toInt, OrderLine(ol(0).toInt, ol(1).toInt, ol(2), ol(3).toInt))
    )


    println("--------")
    val tuple_o_ol = order.cogroup(orderLine).map(x => (x._2._1.head.customerId, (
      x._2._1.head.orderId,
      x._2._1.head.orderName,
      x._2._2.toList
    )))
    tuple_o_ol.foreach(println)

    println("--------")
    val tuple = customer.cogroup(tuple_o_ol)
    tuple.foreach(println)


    val r1 = tuple.map((t) => {
      val custBson = new BasicBSONObject()
      val orderBsonList = new BasicBSONList()
      val orderLineBsonList = new BasicBSONList()

      custBson.put("customerId", t._2._1.head.customerId)
      custBson.put("name", t._2._1.head.name)

      t._2._2.foreach { o =>
        val orderBson = new BasicBSONObject()
        orderBson.put("id", o._1)
        orderBson.put("orderDescription", o._2)

        t._2._2.head._3.foreach { (ol: OrderLine) =>
          val orderLineBson = new BasicBSONObject()
          orderLineBson.put("id", ol.orderLineId)
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
