package sparkfun1.module3

/** Import the spark and math packages */

import org.apache.spark._

import scala.math.random

/** Computes an approximation to pi
  * Command -> spark-submit --class sparkfun1.module3.SparkPi --master local[2] ./cognitive_class_spark_fundamentals_2.11-1.0.jar
  */
object SparkPi {
  def main(args: Array[String]) {
    /** Create the SparkConf object */
    val conf = new SparkConf().setAppName("Spark Pi")
    /** Create the SparkContext */
    val spark = new SparkContext(conf)
    /** business logic to calculate Pi */
    val slices = if (args.length > 0) args(0).toInt else 2
    val n = math.min(100000L * slices, Int.MaxValue).toInt // avoid overflow
    val count = spark.parallelize(1 until n, slices).map { i =>
      val x = random * 2 - 1
      val y = random * 2 - 1
      if (x * x + y * y < 1) 1 else 0
    }.reduce(_ + _)
    /** Printing the value of Pi */
    println("Pi is roughly " + 4.0 * count / n)
    /** Stop the SparkContext */
    spark.stop()
  }
}