import org.apache.hadoop.mapred.{FileOutputCommitter, FileOutputFormat, JobConf}
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

    case class Customer (customerId: Int, name: String, catchPhrase: String, siren: String)
    case class Order (orderId: Int, customerId: Int, orderName: String, createdDate: java.util.Date, updatedDate: java.util.Date, shippedDate: java.util.Date)
    case class Contact (contactId: Int, customerId: Int, firstName: String,  lastName: String, email: String, tel: String, avatar: String)
    case class OrderLine (orderLineId: Int, orderId: Int, name: String, desc: String, cat: String, quantity: Int, unitPrice: Double, lineAmount: Int )

    val customer = sc.textFile(getClass.getResource("fake-customer-qn.csv").toString).map(_.split(";")).map(
      c => (c(0), Customer(c(0).toInt, c(1), c(2), c(3) ))
    )
    val order = sc.textFile(getClass.getResource("fake-order-qn.csv").toString).map(_.split(";")).map(
      o => (o(1), Order(o(0).toInt, o(1).toInt, o(2), format.parse(o(3)), format.parse(o(4)), format.parse(o(5) )))
    )
    val orderLine = sc.textFile(getClass.getResource("fake-orderLine-qn.csv").toString).map(_.split(";")).map(
      ol => (ol(1), OrderLine(ol(0).toInt, ol(1).toInt, ol(2), ol(3), ol(4), ol(5).toInt, ol(6).toDouble, ol(7).toInt))
    )

    //val customerRDD = customer.join(orderC.join(orderLine))
    //customerRDD.foreach(println)

    val r1 = customer.join(order.join(orderLine)).map((tuple) => {
      val custBson = new BasicBSONObject()
      val orderBsonList = new BasicBSONList()

      custBson.put("customerId", tuple._2._1.customerId)

      /*tuple._2._2.head._1.foreach { (o: Order) =>
        val orderBson = new BasicBSONObject()
        orderBson.put("id", o.orderId)
        orderBson.put("customerId", o.customerId)
        orderBsonList.add(orderBson)
      }
      custBson.put("orders", orderBsonList)*/

      (null, custBson)
    })




    r1.saveAsNewAPIHadoopFile("file:///bogus", classOf[Any], classOf[Any], classOf[com.mongodb.hadoop.MongoOutputFormat[Any, Any]], mongoCustomerConf)

  }

}
