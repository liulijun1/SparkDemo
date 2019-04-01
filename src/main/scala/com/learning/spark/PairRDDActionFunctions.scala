package com.learning.spark

object PairRDDActionFunctions {

  def main(args: Array[String]): Unit = {

    val spairRDD = SparkUtils.createRDD("README.md").flatMap(it=>it.split(" ")).map(it=>(it, 1)).filter(it=>(!it._1.contains("http")))

    val collectAsMapResult =  spairRDD.collectAsMap()
    print("\ncollectAsMapResult=")
    collectAsMapResult.foreach{
      it => println(it)
    }

    val countByKeyResult = spairRDD.countByKey()
    print("\ncountByKeyResult=")
    countByKeyResult.foreach {
      it => println(it)
    }

    val lookupResult = spairRDD.lookup("Hadoop")
    print("\nlookupResult=")
    lookupResult.foreach {
      it => println(it)
    }

  }
}
