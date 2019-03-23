package com.learning.spark

import java.util

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

import scala.reflect.ClassTag


object RDDTransformFunctions {

  def main(args: Array[String]): Unit = {
    val distinctRDDWithoutPartition = SparkUtils.createRDD[Int](Seq[Int](1, 2, 3, 4))
    val strParaRDD = SparkUtils.createRDD(Seq[String]("learning", "spark", "rdd", "demo"))
    val repeatData = SparkUtils.createRDD[Int](List(1, 2, 3, 1, 2, 3, 5, 9), 2)
    val dataWithPartition = SparkUtils.createRDD[Int](List(1, 2, 3, 4, 5, 6, 7, 8), 2)
    val strWithPartition = SparkUtils.createRDD(List("One", "Two", "Three", "Four", "Five", "Six", "Seven", "Eight"), 2)
    val hanWithPartition = SparkUtils.createRDD(List("一","二","三","四","五","六","七","八"), 2)

    /*********************************************************************
     *                  Action funtions
     ********************************************************************/
    println(s"""${"\n"}*****************Action functions******************""")
    val aggregateResult = dataWithPartition.aggregate(0)((a, b) => a + b, (c, d) => c + d)
    println(s"aggregateResult=${aggregateResult}")

    val collectResult = dataWithPartition.collect()
    println(s"collectResult=${util.Arrays.toString(collectResult)}")

    val countResult = dataWithPartition.count()
    println(s"countResult=${countResult}")

    val countApproxResult = dataWithPartition.countApprox(100, 0.9)
    println(s"countApproxResult=${countApproxResult}")

    val countByValueResult = dataWithPartition.countByValue()
    print("countByValueResult=")
    countByValueResult.foreach(x => print(s"[value=${x._1}, count=${x._2}],"))

    val firstResult = dataWithPartition.first()
    println(s"\nfirstResult=${firstResult}")

    val foldResult = dataWithPartition.fold(0)((a, b) => a + b)
    println(s"foldResult=${foldResult}")

    print("foreachPartionResult=")
    val foreachPartionResult = dataWithPartition.foreachPartition {
      it =>
        print(s"${it.sum},")
    }

    val getNumPartitionsResult = dataWithPartition.getNumPartitions
    print(s"\ngetNumPartitionsResult=${getNumPartitionsResult}")

    val isEmptyResult = dataWithPartition.isEmpty()
    println(s"\nisEmptyResult=${isEmptyResult}")

    val minResult = dataWithPartition.min()
    println(s"minResult=${minResult}")

    val maxResult = dataWithPartition.max()
    println(s"maxResult=${maxResult}")

    val reduceResult = dataWithPartition.reduce{
      (a,b)=>a+b
    }
    println(s"reduceResult=${reduceResult}")

    val takeResult = dataWithPartition.take(3)
    println(s"takeResult=${util.Arrays.toString(takeResult)}")

    val takeOrderedResult = repeatData.takeOrdered(2)
    println(s"takeOrderedResult=${util.Arrays.toString(takeOrderedResult)}")

    val takeSampleResult = repeatData.takeSample(true, 3)
    println(s"takeSampleResult=${util.Arrays.toString(takeSampleResult)}")

    val topResult = dataWithPartition.top(3)
    println(s"topResult=${util.Arrays.toString(topResult)}")

    val treeAggregateResult = dataWithPartition.treeAggregate(0)((a,b)=>a+b,(c,d)=>c+d, 2)
    println(s"treeAggregateResult=${treeAggregateResult}")

    val treeReduceResult = dataWithPartition.treeReduce((a,b)=>a+b, 2)
    println(s"treeReduceResult=${treeReduceResult}")
  }
}
