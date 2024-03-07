import org.apache.spark.SparkContext._
import scala.io._
import org.apache.spark.{ SparkConf, SparkContext }
import org.apache.spark.rdd._
import org.apache.log4j.Logger
import org.apache.log4j.Level
import scala.collection._

object Main {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)

    val conf = new SparkConf().setAppName("Main").setMaster("local[4]")
    val sc = new SparkContext(conf)

    val airports = Source.fromFile("airports.csv")
      .getLines
      .toList
      .map(_.split(",").map(_.trim))
      .map(arr => Airport(arr(0), arr(1), arr(2), arr(3)))

    val airlines = Source.fromFile("airlines.csv")
      .getLines
      .toList
      .map(_.split(",").map(_.trim))
      .map(arr => Airline(arr(0), arr(1)))

    airlines.foreach(airline => println(airline.mkString(", ")))

    case class Airline(code: String, name: String)
    case class Airport(code: String, name: String, city: String, state: String)
  }

