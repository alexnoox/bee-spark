import org.apache.hadoop.mapred.JobConf
import org.apache.spark.SparkContext._
import org.apache.spark._
import org.bson.BasicBSONObject
import org.bson.types.BasicBSONList


object SAMPLE_CombineByKeyByKey {
  def main(args: Array[String]) {

    // Spark Context setup
    val conf = new SparkConf().setMaster("local").setAppName("Bee-Spark")
    val sc = new SparkContext(conf)

    case class Customer(name: String, amout: Double )

    val input = sc.parallelize(List(
      ("Alex", 10),
      ("Alex", 5),
      ("Tomy", 2)
    ))
    input.foreach(println)


    val result = input.combineByKey(
      (v) => (v, 1), //Turn V to C
      (acc: (Int,Int), v) => (acc._1 + v, acc._2 + 1), //Add V to C
      (acc1: (Int,Int), acc2:(Int,Int)) => (acc1._1 + acc2._1, acc1._2 + acc2._2) // Combine C,C to C
    )

    println("----result combine------")
    result.foreach(println)

    println("----------")
    result.map{case (key, value) => (key, value._1 / value._2.toFloat, value._1, value._1 * value._1, if (value._1 > 10) true else false)}.foreach(println)

  }

}