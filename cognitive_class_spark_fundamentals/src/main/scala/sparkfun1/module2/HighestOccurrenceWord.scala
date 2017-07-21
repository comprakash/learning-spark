package sparkfun1.module2

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

/**
  * This will find highest occurrence work and its frequency
  * Command -> spark-submit --class sparkfun1.module2.HighestOccurrenceWord ./cognitive_class_spark_fundamentals_2.11-1.0.jar ./README.md
  * You can take the Readme file from this location
  * - https://github.com/apache/spark/blob/master/README.md
  *
  * Created by Om Prakash on 7/21/2017.
  */
object HighestOccurrenceWord extends App {
  val inputFile: String = args(0)

  // Create a Scala Spark Context.
  val conf = new SparkConf().setAppName("wordCount").setMaster("local[2]").set("spark.executor.memory","1g")
  val sc = new SparkContext(conf)

  val readme = sc.textFile(inputFile)

  val wordCounts: RDD[(String, Int)] = readme.flatMap(line => line.split(" ")).map(word => (word, 1)).reduceByKey((a, b) => a + b)

  var highestFreq: Int = 0
  var highestFreqWord: String = ""
  wordCounts.filter(_._1.nonEmpty).foreach{
    x =>
      if (x._2>highestFreq) {
        highestFreq=x._2
        highestFreqWord=x._1
      }
  }

  println("Highest Occurrence Word = \"" + highestFreqWord + "\" and its frequency = " + highestFreq)
}
