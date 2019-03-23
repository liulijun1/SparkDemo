package com.learning.spark

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

import scala.reflect.ClassTag

object SparkUtils {

  var sparkContext: SparkContext = new SparkContext("local", "RDDDemo")
  sparkContext.setLogLevel("ERROR")

  def createRDD(input: String): RDD[String] = {
    return sparkContext.textFile(input)
  }

  def createRDD[T:ClassTag](seq:Seq[T]): RDD[T] = {
    return sparkContext.parallelize(seq)
  }

  def createRDD[T:ClassTag](seq:Seq[T], numSlices: Int): RDD[T] = {
    return sparkContext.parallelize(seq, numSlices)
  }

}