package com.learning.spark

import org.apache.spark.storage.StorageLevel

object RDDCacheFunctions {

  def main(args: Array[String]): Unit = {
    val dataWithPartition = SparkUtils.createRDD[Int](1 to 1000000, 10)

    var cachedRDD = dataWithPartition.map(it=>it*10).cache()
    //执行Action函数后保存到缓存
    cachedRDD.count()

    println("*****************cache demo*********************")
    var preTime = System.currentTimeMillis()
    for(i <- 1 to 10)
      cachedRDD.count()
    println(s"cacheUseTime(count)=${System.currentTimeMillis()-preTime}")

    preTime = System.currentTimeMillis()
    for(i <- 1 to 10){
      dataWithPartition.map(it=>it*10).count()
    }
    println(s"notCacheUseTime(count)=${System.currentTimeMillis() - preTime}")

    println(s"storageLevel=${cachedRDD.getStorageLevel}")

    println("*****************persist demo*********************")
    cachedRDD = dataWithPartition.filter(a=>a%2==0).persist(StorageLevel.MEMORY_AND_DISK)
    cachedRDD.count()
    preTime=System.currentTimeMillis()
    for(i <- 1 to 10)
      cachedRDD.sum()
    println(s"cacheUseTime(sum)=${System.currentTimeMillis()-preTime}")

    preTime = System.currentTimeMillis()
    for(i <- 1 to 10)
      dataWithPartition.filter(a=>a%2==0).sum()
    println(s"notCacheUseTime(sum)=${System.currentTimeMillis()-preTime}")


    println("*****************saveAsObjectFile demo*********************")
    dataWithPartition.saveAsObjectFile("./saveAsObejctFile")

    println("*****************saveAsTextFile demo*********************")
    dataWithPartition.saveAsTextFile("./saveAsTextFile")
  }
}
