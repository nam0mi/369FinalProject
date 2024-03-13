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

import java.io.{File, PrintWriter}
import scala.collection._
import scala.util.Try
import scala.util.matching.Regex

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
        case "True" => true
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
        val x = if (bool1) 1 else 0
        val y = if (bool2) 1 else 0
        math.pow(y - x, 2) }.sum)
  }

  def predictWithKNN(query: EncodedFlight, dataset: RDD[EncodedFlight], k: Int): Int = {
    val neighbors = dataset.map(flight => (euclideanDistance(query.features, flight.features), flight.isDelayed))
      .sortByKey()
      .take(k+1)
      .tail
    val delays = neighbors.map(_._2).sum
    if (delays.toDouble / k > 0.5) 1 else 0
  }

  def calculateAccuracy(predictionsAndActual: Seq[(Int, Int)]): Double = {
    val totalExamples = predictionsAndActual.length
    if (totalExamples == 0) {
      println("Error: Empty input sequence.")
      return 0.0
    }

    val correctPredictions = predictionsAndActual.count { case (predicted, actual) => predicted == actual }
    val accuracy = correctPredictions.toDouble / totalExamples.toDouble * 100.0
    accuracy
  }

  def calculatePrecision(predictionsAndActual: Seq[(Int, Int)]): Double = {
    val truePositives = predictionsAndActual.count { case (predicted, actual) => predicted == 1 && actual == 1 }
    val falsePositives = predictionsAndActual.count { case (predicted, actual) => predicted == 1 && actual == 0 }

    if (truePositives + falsePositives == 0) {
      println("Error: No positive predictions.")
      return 0.0
    }

    val precision = truePositives.toDouble / (truePositives + falsePositives).toDouble
    precision
  }

  def calculateRecall(predictionsAndActual: Seq[(Int, Int)]): Double = {
    val truePositives = predictionsAndActual.count { case (predicted, actual) => predicted == 1 && actual == 1 }
    val falseNegatives = predictionsAndActual.count { case (predicted, actual) => predicted == 0 && actual == 1 }

    if (truePositives + falseNegatives == 0) {
      println("Error: No actual positive cases.")
      return 0.0
    }

    val recall = truePositives.toDouble / (truePositives + falseNegatives).toDouble
    recall
  }

  def calculateF1Score(predictionsAndActual: Seq[(Int, Int)]): Double = {
    val precision = calculatePrecision(predictionsAndActual)
    val recall = calculateRecall(predictionsAndActual)

    if (precision + recall == 0) {
      println("Error: Precision and recall both are zero.")
      return 0.0
    }

    val f1Score = 2 * (precision * recall) / (precision + recall)
    f1Score
  }

  def generateConfusionMatrix(predictionsAndActual: Seq[(Int, Int)]): Array[Array[Int]] = {
    val matrixSize = 2
    val confusionMatrix = Array.ofDim[Int](matrixSize, matrixSize)

    for ((predicted, actual) <- predictionsAndActual) {
      confusionMatrix(actual)(predicted) += 1
    }
    confusionMatrix
  }


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

    val encodedflights = sc.textFile("src/flights_encoded_small_sample.csv").map(parseEncodedFlight)

    val Array(trainingData, testData) = encodedflights.randomSplit(Array(0.7, 0.3))

    val writer = new PrintWriter(new File("predictions_vs_actuals.txt"))
    testData.collect().foreach { testInstance =>
      val prediction = predictWithKNN(testInstance, trainingData, k = 5)
      writer.println(s"Predicted: $prediction, Actual: ${testInstance.isDelayed}")
      println(s"Predicted: $prediction, Actual: ${testInstance.isDelayed}")
    }
    writer.close()

    val result: Seq[(Int, Int)] = sc.textFile("C:\\Users\\13035\\OneDrive\\Documents\\369FinalProject\\src\\predictions_vs_actuals.txt")
      .flatMap { line =>
        val pattern = "Predicted: (\\d+), Actual: (\\d+)".r
        line match {
          case pattern(predicted, actual) => Some((predicted.toInt, actual.toInt))
          case _ => None // Ignore lines that don't match the pattern
        }
      }
      .collect()
      .toSeq

    val accuracy = calculateAccuracy(result)
    val precision = calculatePrecision(result)
    val recall = calculateRecall(result)
    val f1score = calculateF1Score(result)
    val confusionMatrix = generateConfusionMatrix(result)

    sc.stop()

  }
}

