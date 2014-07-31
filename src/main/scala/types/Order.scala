package types

case class Order (orderId: Int, customerId: Int, description: String, amount: Int)

object Order {
  def fromCsv(line: Array[String]): Order = {
    Order(line(0).toInt, line(1).toInt, line(2), line(3).toInt)
  }

  def toMap(order: Order): Map[String, String] = {
    val fields = Map(
      "orderId" -> order.orderId.toString,
      "customerId" -> order.customerId.toString,
      "description" -> order.description,
      "amout" -> order.amount.toString
    )
    fields
  }
}