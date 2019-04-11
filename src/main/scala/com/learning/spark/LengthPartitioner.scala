package com.learning.spark

import org.apache.spark.Partitioner

class LengthPartitioner(numPartions: Int) extends Partitioner{
  override def numPartitions: Int = numPartions

  override def getPartition(key: Any): Int = {
    if (key.isInstanceOf[String]){
      return key.asInstanceOf[String].length % numPartions
    }

    return 0
  }

  override def equals(other: Any): Boolean = other match {
    case lp: LengthPartitioner =>
      lp.numPartitions == numPartitions
    case _ =>
      false
  }
}
