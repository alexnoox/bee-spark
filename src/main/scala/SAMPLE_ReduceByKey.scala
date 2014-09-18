import org.apache.hadoop.mapred.JobConf
import org.apache.spark.SparkContext._
import org.apache.spark._
import org.bson.BasicBSONObject
import org.bson.types.BasicBSONList


object SAMPLE_ReduceByKey {
  def main(args: Array[String]) {

    // Spark Context setup
    val conf = new SparkConf().setMaster("local").setAppName("Bee-Spark")
    val sc = new SparkContext(conf)

    case class Customer(customerId: Int, name: String, order: Order)
    case class Order(orderId: Int, customerId: Int, orderName: String, orderLine: OrderLine)
    case class OrderLine(orderLineId: Int, orderId: Int, lineAmount: Int)

    println("############### r #########################")

    val tuple = Seq[(Customer)](
      new Customer(1, "alex", new Order(1, 1, "a_", new OrderLine(1, 1, 91))),
      new Customer(1, "alex", new Order(2, 1, "aa", new OrderLine(2, 2, 92))),
      new Customer(1, "alex", new Order(2, 1, "aa", new OrderLine(2, 2, 92))),
      new Customer(2, "fred", new Order(3, 2, "f_", new OrderLine(4, 3, 94))),
      new Customer(2, "fred", new Order(4, 2, "ff", new OrderLine(5, 4, 95))),
      new Customer(2, "fred", new Order(4, 2, "ff", new OrderLine(6, 4, 96)))
    )

    println("----")
    val pairs = sc.parallelize(tuple).map(x => (x, 1))
    pairs.foreach(println)


    println("----")
    val counts = pairs.reduceByKey((a, b) => a + b)
    counts.foreach(println)


  }

}