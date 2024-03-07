import org.apache.spark.SparkContext._
import scala.io._
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd._
import org.apache.log4j.Logger
import org.apache.log4j.Level
import shapeless.syntax.std.tuple.productTupleOps
import scala.collection._

object Main {
  case class Airline(code: String, name: String)
  case class Airport(code: String, name: String, city: String, state: String)
  case class Flight(year: Int, month: Int, day: Int, day_of_week: Int, airline: String, flight_num: Int,
                    origin: String, dest: String, scheduled_depart: Int, actual_depart: Int, delay: Int)

  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)

    val conf = new SparkConf().setAppName("Main").setMaster("local[4]")
    val sc = new SparkContext(conf)

    val airports = sc.textFile("src/airports.csv")
      .map(_.split(",").map(_.trim))
      .map(arr => Airport(arr(0), arr(1), arr(2), arr(3)))

    val airlines = sc.textFile("src/airlines.csv")
      .map(_.split(",").map(_.trim))
      .map(arr => Airline(arr(0), arr(1)))

    val flights = sc.textFile("src/flights.csv")
      .map(_.split(",").map(_.trim))
      .filter(_(0) != "YEAR")
      .filter(arr => !arr.exists(_.isEmpty))
      .map(arr => Flight(arr(0).toInt, arr(1).toInt, arr(2).toInt, arr(3).toInt, arr(4), arr(5).toInt,
        arr(7), arr(8), arr(9).toInt, arr(10).toInt, arr(11).toInt))

    //airlines.foreach(airline => println(airline.mkString("", ", ", "")))
    //airports.foreach(airport => println(airport.mkString("", ", ", "")))
    //flights.foreach(flight => println(flight.mkString("", ", ", "")))
  }
}

