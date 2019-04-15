package com.learning.spark

import org.apache.hadoop.mapred.InputFormat
import org.apache.spark.SparkContext
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.util.LongAccumulator

import scala.reflect.ClassTag

object SparkUtils {

  var sparkContext: SparkContext = new SparkContext("local", "RDDDemo")
  sparkContext.setLogLevel("ERROR")

  def createRDD(input: String): RDD[String] = {
    return sparkContext.textFile(input)
  }

  def readSequenceFile[K: ClassTag, V: ClassTag](input: String, keyClass: Class[K], valueClass: Class[V]): RDD[(K, V)] = {
    return sparkContext.sequenceFile(input, keyClass, valueClass)
  }

  def readFileByOldHadoopApi[K: ClassTag, V: ClassTag, F <: InputFormat[K, V]](input: String)(implicit fm: ClassTag[F]): RDD[(K, V)] = {
    return sparkContext.hadoopFile[K, V, F](input)
  }

  def readFileByNewHadoopApi[K, V, F <:org.apache.hadoop.mapreduce.InputFormat[K, V]](
                                                         path: String,
                                                         fClass: Class[F],
                                                         kClass: Class[K],
                                                         vClass: Class[V]): RDD[(K, V)] =  {
    return sparkContext.newAPIHadoopFile(path, fClass, kClass, vClass)

  }

  def createRDD[T: ClassTag](seq: Seq[T]): RDD[T] = {
    return sparkContext.parallelize(seq)
  }

  def createRDD[T: ClassTag](seq: Seq[T], numSlices: Int): RDD[T] = {
    return sparkContext.parallelize(seq, numSlices)
  }

  def createHiveContext(): HiveContext = {
    return new HiveContext(sparkContext)
  }

  def createLongAccumulator(): LongAccumulator = {
    return sparkContext.longAccumulator
  }

  def createBroadcast[T](values: T): Broadcast[T] = {
    return sparkContext.broadcast(values)
  }
}