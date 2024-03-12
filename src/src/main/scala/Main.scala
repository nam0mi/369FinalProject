import scala.math._
import org.apache.spark.SparkContext._
import org.apache.spark._

import scala.io._
import org.apache.spark.sql._
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd._
import org.apache.log4j.Logger
import org.apache.log4j.Level
import shapeless.syntax.std.tuple.productTupleOps

import scala.collection._
import scala.util.Try

object Main {
  case class Airline(code: String, name: String)
  case class Airport(code: String, name: String, city: String, state: String)
  case class Flight(year: Int, month: Int, day: Int, day_of_week: Int, airline: String, flight_num: Int,
                    origin: String, dest: String, scheduled_depart: Int, actual_depart: Int, delay: Int, is_delayed: Boolean)
  case class EncodedFlight(features: Array[Boolean], isDelayed: Int)
  def parseEncodedFlight(line: String): EncodedFlight = {
    val parts = line.trim.split(",").map(_.trim)
    if (parts.length > 1) {
      val features = parts.slice(1, parts.length).map {
        case "1" => true
        case _ => false
      }
      Try(parts(0).toDouble).toOption match {
        case Some(delay) =>
          val isDelayed = if (delay > 10.0) 1 else 0
          EncodedFlight(features, isDelayed)
        case None => EncodedFlight(Array(true), 0) // Handle the case where conversion to Double fails
      }
    } else EncodedFlight(Array(true), 0) // Handle the case where there are not enough parts
  }
  def euclideanDistance(a: Array[Boolean], b: Array[Boolean]): Double = {
    math.sqrt(a.zip(b)
      .map { case (bool1, bool2) =>
      var x = 0
      var y = 0
        if(bool1)
        {
          x = 1
          println("GOT HERE" + x)
        }
        if(bool2)
        {
          y = 1
          println("GOT HERE" + y)
        }
        math.pow(y - x, 2) }.sum)
  }

  def predictWithKNN(query: EncodedFlight, dataset: RDD[EncodedFlight], k: Int): Int = {
    val neighbors = dataset.map(flight => (euclideanDistance(query.features, flight.features), flight.isDelayed))
      .sortByKey()
      .take(k)
    val delays = neighbors.map(_._2).sum
    if (delays.toDouble / k > 0.5) 1 else 0
  }

  //  def distance(flight1: Flight, flight2: Flight): Double = {
//    sqrt(
//      pow(flight1.day_of_week - flight2.day_of_week, 2) +
//        pow(flight1.delay - flight2.delay, 2) +
//        pow(flight1.airline.toDouble - flight2.airline.toDouble, 2) + // Assuming airline, origin, and dest are now numeric
//        pow(flight1.origin.toDouble - flight2.origin.toDouble, 2) +
//        pow(flight1.dest.toDouble - flight2.dest.toDouble, 2)
//    )
//  }
//
//  def knn(queryFlight: Flight, trainingData: RDD[Flight], k: Int): Boolean = {
//    val neighbors = trainingData.map(flight => (distance(queryFlight, flight), flight))
//      .sortByKey()
//      .take(k)
//
//    val delays = neighbors.count(_._2.is_delayed)
//    delays > k / 2
//  }
//
//  def predictDelays(flightsEncoded: RDD[Flight], k: Int): RDD[(Flight, Boolean)] = {
//    flightsEncoded.map { queryFlight =>
//      (queryFlight, knn(queryFlight, flightsEncoded, k))
//    }
//  }


  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)

    val conf = new SparkConf().setAppName("NameOfApp").setMaster("local[4]")
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
        arr(7), arr(8), arr(9).toInt, arr(10).toInt, arr(11).toInt, arr(11).toInt >= 15 ))

    val encodedflights = sc.textFile("src/flights_encoded_condensed.csv").map(parseEncodedFlight)
    //val encodedflights = sc.textFile("src/small_encoded_copy.csv").map(parseEncodedFlight)



    // Extract unique values for categorical features and zip with unique index
//    val airlinesIndex = flights.map(_.airline).distinct().zipWithIndex().collectAsMap()
//    val originsIndex = flights.map(_.origin).distinct().zipWithIndex().collectAsMap()
//    val destinationsIndex = flights.map(_.dest).distinct().zipWithIndex().collectAsMap()

    val Array(trainingData, testData) = encodedflights.randomSplit(Array(0.7, 0.3))

//    val predictions = testData.map { case flight =>
//      (flight, predictWithKNN(flight, trainingData, k = 5))
//    }

    testData.collect().foreach { testInstance =>
      val prediction = predictWithKNN(testInstance, trainingData, k = 5)
      println(s"Predicted: $prediction, Actual: ${testInstance.isDelayed}")
    }

    // Calculate accuracy
//    val correct = predictions.filter { case (original, prediction) => original.isDelayed == prediction }.count()
//    val accuracy = correct.toDouble / testData.count()
//
//    println(s"Accuracy: $accuracy")


    // Broadcast the mappings to use them in RDD transformations
//    val airlinesIndexBC = sc.broadcast(airlinesIndex)
//    val originsIndexBC = sc.broadcast(originsIndex)
//    val destinationsIndexBC = sc.broadcast(destinationsIndex)

    //actually encode the flights data
//    val flightsEncoded = flights.map { flight =>
//      val airlineId = airlinesIndexBC.value(flight.airline)
//      val originId = originsIndexBC.value(flight.origin)
//      val destId = destinationsIndexBC.value(flight.dest)
//      flight.copy(airline = airlineId.toString, origin = originId.toString, dest = destId.toString)
//    }

    // Split the data into training and testing sets
    //val Array(trainingData, testData) = flights.randomSplit(Array(0.7, 0.3))

    // Predict delays on the test data
    //val predictions = predictDelays(flightsEncoded, 5) // Using k=5 for this example

    // Example of how to print out predictions
//    predictions.foreach { case (flight, predictedDelay) =>
//      println(s"Flight: ${flight.flight_num}, Predicted Delay: $predictedDelay")
//    }


    //airlines.foreach(airline => println(airline.mkString("", ", ", "")))
    //airports.foreach(airport => println(airport.mkString("", ", ", "")))
    //flights.foreach(flight => println(flight.mkString("", ", ", "")))

  }
}

