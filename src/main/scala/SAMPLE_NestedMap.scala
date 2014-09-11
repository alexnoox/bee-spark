import org.apache.hadoop.mapred.{FileOutputCommitter, FileOutputFormat, JobConf}
import org.apache.spark.SparkContext._
import org.apache.spark._
import org.bson.BasicBSONObject
import org.bson.types.BasicBSONList

object SAMPLE_NestedMap {
  def main(args: Array[String]) {

    // Spark Context setup
    val conf = new SparkConf().setMaster("local").setAppName("Bee-Spark")
    val sc = new SparkContext(conf)
    //sc.setLocalProperty("spark.serializer", classOf[KryoSerializer].getName)

    val mongoCustomerConf = new JobConf(sc.hadoopConfiguration)
    mongoCustomerConf.set("mongo.output.uri", "mongodb://127.0.0.1:27017/abo.customer")


    case class Customer (customerId: Int, name: String, order: Order)
    case class Order (orderId: Int, customerId: Int, orderName: String, orderLine: OrderLine)
    case class OrderLine (orderLineId: Int, orderId: Int, lineAmount: Int )

    println("############### r #########################")

    val tuple = Seq[(Customer)](
    new Customer(1, "alex", new Order(1,1,"a_", new OrderLine(1,1,91)) ),
    new Customer(1, "alex", new Order(2,1,"aa", new OrderLine(2,2,92)) ),
    new Customer(1, "alex", new Order(2,1,"aa", new OrderLine(3,2,93)) ),
    new Customer(2, "fred", new Order(3,2,"f_", new OrderLine(4,3,94)) ),
    new Customer(2, "fred", new Order(4,2,"ff", new OrderLine(5,4,95)) ),
    new Customer(2, "fred", new Order(4,2,"ff", new OrderLine(6,4,96)) )
    )

    println("----")
    //val r1 = tuple.groupBy(_.customerId)
    tuple.foreach(println)

    println("----")
    val r2 = sc.parallelize(tuple).map(x => (x.customerId, (x.customerId, x.name, x.order.orderLine.lineAmount)))
    r2.foreach(println)

    println("----")
    val r3 = r2.reduceByKey((a, b) => (b._1, b._2, (b._3 + b._3)))
    r3.foreach(println)

    println("############### s #########################")

    val tuple1 = Seq[Int](1,2,3,4,2,3)


    println("----")
    val s1 = sc.parallelize(tuple1).map(x => (x, 1))
    s1.foreach(println)

    println("----")
    val s2 = s1.reduceByKey((a, b) => a + b)
    s2.foreach(println)

    println("############### r3 #########################")



  }
}