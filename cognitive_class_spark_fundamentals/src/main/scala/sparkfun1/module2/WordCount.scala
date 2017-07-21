package sparkfun1.module2

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * This will count the number of occurrences of each word
  * Command -> spark-submit --class sparkfun1.module2.WordCount ./cognitive_class_spark_fundamentals_2.11-1.0.jar ./README.md
  * You can take the Readme file from this location
  * - https://github.com/apache/spark/blob/master/README.md
  *
  * Created by Om Prakash on 7/21/2017.
  */
object WordCount extends App {

  val inputFile: String = args(0)
  //val outputDirectory: String = args(1)

  // Create a Scala Spark Context.
  val conf = new SparkConf().setAppName("wordCount").setMaster("local[2]").set("spark.executor.memory","1g")
  val sc = new SparkContext(conf)

  val readme = sc.textFile(inputFile)

  // Store the file contents in memory
  readme.cache()

  println("Total lines in file = " + readme.count)

  println("First Line = " + readme.first)

  val linesWithSpark = readme.filter(line => line.contains("Spark"))

  println("Lines with Spark keyword in it = " + linesWithSpark.count())

  val mostWordsInLine: Int = readme.map(line => line.split(" ").length).reduce((a, b) => if (a > b) a else b)

  println("Max number of words in a line = " + mostWordsInLine)

  val wordCounts: RDD[(String, Int)] = readme.flatMap(line => line.split(" ")).map(word => (word, 1)).reduceByKey((a, b) => a + b)

  println("Words and their frequency")
  wordCounts.collect().foreach(println)
}
