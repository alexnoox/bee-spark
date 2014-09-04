import org.apache.hadoop.mapred.{FileOutputCommitter, FileOutputFormat, JobConf}
import org.apache.spark.SparkContext._
import org.apache.spark._

object SAMPLE_NestedMap {
  def main(args: Array[String]) {

    // Spark Context setup
    val conf = new SparkConf().setMaster("local").setAppName("Bee-Spark")
    val sc = new SparkContext(conf)

    val format = new java.text.SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss")

    case class Customer (customerId: Int, name: String, catchPhrase: String, siren: String)
    case class Order (orderId: Int, customerId: Int, orderName: String, createdDate: java.util.Date, updatedDate: java.util.Date, shippedDate: java.util.Date)
    case class Contact (contactId: Int, customerId: Int, firstName: String,  lastName: String, email: String, tel: String, avatar: String)
    case class OrderLine (orderLineId: Int, orderId: Int, name: String, desc: String, cat: String, quantity: Int, unitPrice: Double, lineAmount: Int )

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
      ol => (ol(1), OrderLine(ol(0).toInt, ol(1).toInt, ol(2), ol(3), ol(4), ol(5).toInt, ol(6).toDouble, ol(7).toInt))
    )



    println("########################################")
    val customerRDD = customer.join(orderC)collect()
    customerRDD.foreach(println)

    println("########################################")
    val customerRDD2 = customerRDD.groupBy(_._1).mapValues(
      _.groupBy(_._2).mapValues(_.groupBy(_._2))
    )
    customerRDD2.foreach(println)

    type customerId = Int
    type customerName = String
    type orderId = Int
    type orderName = String
    type amout = Double

    val tupleSeq = Seq[(customerId,customerName,orderId,orderName,amout)](
      (1,"customerA",1,"orderA",150),
      (1,"customerA",2,"orderB",250),
      (1,"customerA",3,"orderC",350),
      (1,"customerA",4,"orderD",450),
      (2,"customerB",1,"orderA",550),
      (2,"customerB",2,"orderB",650),
      (2,"customerB",3,"orderC",750),
      (2,"customerB",4,"orderD",850),
      (2,"customerB",5,"orderE",950),
      (2,"customerB",6,"orderF",1050)
    )

    println("--> " + tupleSeq.groupBy(_._1))
    println("--> " + tupleSeq.groupBy(x => x._1))
    println("--> " + tupleSeq.groupBy(x => x._2))
    println("--> " + tupleSeq.groupBy(x => x._4))

    println("--> " + tupleSeq.groupBy(x => x._1).mapValues(_.groupBy(_._2)))

    println("--> " +
      tupleSeq.groupBy(_._1).mapValues(
        _.groupBy(_._2).mapValues(
          _.groupBy(_._3).mapValues{ case Seq(p) => p._4 }
        )
      )
    )

  }
}