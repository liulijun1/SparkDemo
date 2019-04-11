package com.learning.spark

import java.util

import org.apache.hadoop.mapred.lib.HashPartitioner
import org.apache.spark
import org.apache.spark.TaskContext

object RDDPartitionFunctions {

  def main(args: Array[String]): Unit = {
    val pairRDD = SparkUtils.createRDD("README.md").flatMap(it=>it.split(" ")).map(it=>(it, 1)).filter(it=>(!it._1.contains("http")))

    val partitionsResult = pairRDD.partitions
    print(s"\npartitionsResult=")
    partitionsResult.foreach {
      it=>print(s"${it.index},")
    }

    val partitionByResult = pairRDD.partitionBy(new spark.HashPartitioner(100))
    print("\npartitionByResult=")
    partitionByResult.partitions.foreach{
      it=>print(s"${it.index},")
    }

    val partitionerResult = partitionByResult.partitioner
    print(s"\npartitionerResult=${partitionerResult}")

    val selfPartitionerResult = pairRDD.partitionBy(new LengthPartitioner(10))
    print("\nselfPartitionerResult=")
    selfPartitionerResult.foreachPartition{
      i=> {
        print(s"\n${TaskContext.get.partitionId}:")
        while(i.hasNext){
          print(s"${i.next()}")
        }
      }
    }

  }
}
