package types

case class Customer (customerId: Int, name: String, catchPhrase: String, siren: String)

object Customer {
  def fromCsv(line: Array[String]): Customer = {
    Customer(line(0).toInt, line(1), line(2), line(3))
  }

  def toMap(customer: Customer): Map[String, String] = {
    val fields = Map(
      "id" -> customer.customerId.toString,
      "name" -> customer.name,
      "catchPhrase" -> customer.catchPhrase,
      "siren" -> customer.siren
    )
    fields
  }
}