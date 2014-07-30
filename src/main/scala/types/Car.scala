package types

case class Car (year: Int, make: String, model: String, desc: String,  price: Double)

object Car {
  def fromCsv(line: Array[String]): Car = {
    Car(line(0).toInt, line(1), line(2), line(3), line(4).toDouble)
  }
}
