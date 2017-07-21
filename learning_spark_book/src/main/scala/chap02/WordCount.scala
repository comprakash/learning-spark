package chap02

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

/**
  * This will count the number of occurrences of each word
  * Command -> spark-submit --class chap02.WordCount ./learning_spark_book_2.11-1.0.jar ./README.md ./output
  * You can take the Readme file from this location
  * - https://github.com/apache/spark/blob/master/README.md
  *
  * Created by Om Prakash on 7/20/2017.
  */
object WordCount extends App {

  val inputFile: String = args(0)
  val outputDirectory: String = args(1)

  // Create a Scala Spark Context.
  val conf = new SparkConf().setAppName("wordCount").setMaster("local[2]").set("spark.executor.memory","1g")
  val sc = new SparkContext(conf)
  // Load our input data.
  val input = sc.textFile(inputFile)
  // Split it up into words.
  val words = input.flatMap(line => line.split(" "))
  // Transform into pairs and count.
  val counts = words.map(word => (word, 1)).reduceByKey{case (x, y) => x + y}
  // Save the word count back out to a text file, causing evaluation.
  counts.saveAsTextFile(outputDirectory)

  println("Stored the word count results to directory - " + outputDirectory)

}
