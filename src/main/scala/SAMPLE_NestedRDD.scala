import org.apache.hadoop.mapred.JobConf
import org.apache.spark.SparkContext._
import org.apache.spark._

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
      c => (c(0), Customer(c(0).toInt, c(1) ))
    )
    val order = sc.textFile(getClass.getResource("fake-order-qn.csv").toString).map(_.split(";")).map(
      o => (o(1), Order(o(0).toInt, o(1).toInt, o(2) ))
    )
    val orderLine = sc.textFile(getClass.getResource("fake-orderLine-qn.csv").toString).map(_.split(";")).map(
      ol => (ol(1), OrderLine(ol(0).toInt, ol(1).toInt, ol(2), ol(3).toInt))
    )

    val tuple = customer.cogroup(order).map(x => (x._2._2.head.orderId, (
        x._2._1.head.customerId,
        x._2._1.head.name,
        x._2._2.toList
      )))


    tuple.foreach(println)


    //val res = tuple.cogroup(orderLine)


    /*val tuple

    println("----")
    val r2 = sc.parallelize(tuple.map(x => (x._2._1.customerId, (x._2._1.name, x._2._1.customerId, x._2._2._1.orderName, x._2._2._2.lineAmount) ) ) )
    r2.foreach(println)

    println("----")
    val r3 = r2.reduceByKey((a, b) => (b._1, b._2, b._3, (a._4 + b._4)))
    r3.foreach(println)

*/

    /*val r1 = customer.join(order.join(orderLine)).map((tuple) => {
      val custBson = new BasicBSONObject()
      val orderBsonList = new BasicBSONList()

      custBson.put("customerId", tuple._2._1.customerId)
      (null, custBson)
    })

    r1.saveAsNewAPIHadoopFile("file:///bogus", classOf[Any], classOf[Any], classOf[com.mongodb.hadoop.MongoOutputFormat[Any, Any]], mongoCustomerConf)
    */

  }

}
