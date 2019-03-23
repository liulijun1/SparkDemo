package com.learning.spark

import org.apache.spark.{SparkConf, SparkContext}

/**
  * first spark program
  */
object HelloSpark {

  /**
    *
    * @param args set input file name and output dir name
    */
  def main(args: Array[String]): Unit = {
    if (args.length < 2) {
      println("please set input file and output file")
      System.exit(1)
    }

    val sparkConf = new SparkConf().setMaster("local").setAppName("HelloSpark")
    val sc = new SparkContext(sparkConf)
    val input = sc.textFile(args(0))
    val words = input.flatMap(line => line.split(" "))
    val counts = words.map(word => (word, 1)).reduceByKey {
      case (x, y) => x + y
    }

    counts.saveAsTextFile(args(1))

    sc.stop()
  }
}
