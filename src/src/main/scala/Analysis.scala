import org.apache.spark.SparkContext._
import scala.io._
import org.apache.spark.{ SparkConf, SparkContext }
import org.apache.spark.rdd._
import org.apache.log4j.Logger
import org.apache.log4j.Level
import scala.collection._

object Analysis {
  def main(args: Array[String]): Unit = {
    System.setProperty("hadoop.home.dir", "c:/winutils/")
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)

    val conf = new SparkConf().setAppName("Analysis").setMaster("local[4]")
    val sc = new SparkContext(conf)

    case class Airline(code: String, name: String)
    case class Airport(code: String, name: String, city: String, state: String)
    case class Flight(year: Int, month: Int, day: Int, day_of_week: Int, airline: String, flight_num: Int,
                      origin: String, dest: String, scheduled_depart: Int, actual_depart: Int, delay: Int, is_delayed: Boolean)

    val airports = sc.textFile("src/airports.csv")
      .map(_.split(",").map(_.trim))
      .map(arr => Airport(arr(0), arr(1), arr(2), arr(3)))

    val airlines = sc.textFile("src/airlines.csv")
      .map(_.split(",").map(_.trim))
      .map(arr => Airline(arr(0), arr(1)))

    val flights = sc.textFile("src/flights.csv")
      .map(_.split(",").map(_.trim))
      .filter(_ (0) != "YEAR")
      .filter(arr => !arr.exists(_.isEmpty))
      .map(arr => Flight(arr(0).toInt, arr(1).toInt, arr(2).toInt, arr(3).toInt, arr(4), arr(5).toInt,
        arr(7), arr(8), arr(9).toInt, arr(10).toInt, arr(11).toInt, arr(11).toInt >= 15))
    //Determine departure airports average delay
    val airportTup = airports.map(airport => (airport.code, airport.name))
    val flightsTup1 = flights.map(flight => (flight.origin, flight.delay))

    val top1 = flightsTup1.leftOuterJoin(airportTup)
    top1.map {
      case (airportCode, (delay, Some(airportName))) => (airportName, delay)
      case (airlineCode, (delay, None)) => ("Ignore", delay)
    }.filter(_._1 != "Ignore").groupByKey().mapValues(diff => diff.aggregate((0, 0))(
      (r1, r2) => (r1._1 + r2, r1._2 + 1),
      (r1, r2) => (r1._1 + r2._1, r1._2 + r2._2)
    )).map { case (name, (sum, count)) => (name, sum.toDouble / count) }.collect().sortBy(_._2)//.foreach(println)

    //Determine average delay for each airline
    val airlineTup = airlines.map(airline => (airline.code, airline.name))
    val flightsTup2 = flights.map(flight => (flight.airline, flight.delay))

    val top2 = flightsTup2.leftOuterJoin(airlineTup)
    top2.map{
      case(airlineCode, (delay, Some(airlineName))) => (airlineName, delay)
        case(airlineCode, (delay, None)) => ("Ignore", delay)
    }.filter(_._1!="Ignore").groupByKey().mapValues (diff => diff.aggregate((0, 0))(
      (r1, r2) => (r1._1 + r2, r1._2 + 1),
      (r1, r2) => (r1._1 + r2._1, r1._2 + r2._2)
    )).map{case(name, (sum, count)) => (name, sum.toDouble/count)}.collect().sortBy(_._2)//.foreach(println)

    // Average delay by month
    val month = flights.map(flight => (flight.month, flight.delay))
    month.groupByKey().mapValues(diff => diff.aggregate((0, 0))(
      (r1, r2) => (r1._1 + r2, r1._2 + 1),
      (r1, r2) => (r1._1 + r2._1, r1._2 + r2._2)
    )).map { case (name, (sum, count)) => (name, sum.toDouble / count) }.collect().foreach(println)
  }
}
