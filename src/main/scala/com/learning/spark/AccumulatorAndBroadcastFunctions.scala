package com.learning.spark

object AccumulatorAndBroadcastFunctions {
  def main(args: Array[String]): Unit = {
    val textRDD = SparkUtils.createRDD("README.md")

    val lineAccumulator = SparkUtils.createLongAccumulator()

    val filterTextRDD = textRDD.filter {
      line=>
        lineAccumulator.add(1)
        line.toLowerCase.contains("spark")
    }

    println("---------------longAccumulator----------------")
    println("include spark text lines number=" + filterTextRDD.collect().length)
    println("all text lines number=" + lineAccumulator.value)

    

  }

}
