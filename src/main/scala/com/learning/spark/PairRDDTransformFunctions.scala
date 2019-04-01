package com.learning.spark

object PairRDDTransformFunctions {

  def main(args: Array[String]): Unit = {

    val pairRDD = SparkUtils.createRDD("README.md").flatMap(it=>it.split(" ")).map(it=>(it, 1)).filter(it=>(!it._1.contains("http")))

    val aggregateByKeyResult = pairRDD.aggregateByKey(0)((a,b)=>a+b, (c,d)=>c+d)
    print("\naggregateByKeyResult=")
    aggregateByKeyResult.collect().foreach{
      it=>print(s"${it},")
    }

    val cogroupResut = pairRDD.cogroup(pairRDD)
    print("\ncogroupResut=")
    cogroupResut.collect().foreach {
      it => print(s"${it},")
    }

    val foldByKeyResult = pairRDD.foldByKey(0) {
      (a,b) => a+b
    }
    print("\nfoldByKeyResult=")
    foldByKeyResult.collect().foreach{
      it=>print(s"${it},")
    }

    case class Juice(volumn: Int) {
      def add(j: Juice):Juice = Juice(volumn + j.volumn)
    }

    case class Fruit(kind: String, weight: Int) {
      def makeJuice:Juice = Juice(weight * 100)
    }

    val apple1 = Fruit("apple", 5)
    val apple2 = Fruit("apple", 8)
    val orange1 = Fruit("orange", 10)

    val fruit = SparkUtils.createRDD(List(("apple", apple1) , ("orange", orange1) , ("apple", apple2)))
    val combineByKeyResult = fruit.combineByKey(
      f => f.makeJuice,
      (j:Juice, f) => j.add(f.makeJuice),
      (j1:Juice, j2:Juice) => j1.add(j2)
    )
    print("\ncombineByKeyResult=")
    combineByKeyResult.collect().foreach {
      it=>print(s"${it},")
    }

    print("\n求平均成绩：")
    val initialScores = Array(("Fred", 88.0), ("Fred", 95.0), ("Fred", 91.0), ("Wilma", 93.0), ("Wilma", 95.0), ("Wilma", 98.0))
    val d1 = SparkUtils.createRDD(initialScores)
    type MVType = (Int, Double) //定义一个元组类型(科目计数器,分数)
    d1.combineByKey(
      score => (1, score),
      (c1: MVType, newScore) => (c1._1 + 1, c1._2 + newScore),
      (c1: MVType, c2: MVType) => (c1._1 + c2._1, c1._2 + c2._2)
    ).map { case (name, (num, socre)) => (name, socre / num) }.collect.foreach{
      it=>print(s"${it},")
    }

    val flatRDD = SparkUtils.createRDD("README.md").flatMap(it=>it.split(" ")).map(it=>(it, it)).filter(it=>(it._1.contains("Hadoop")))
    val flatMapValuesResult = flatRDD.flatMapValues(v=>v.toCharArray)
    print("\nflatMapValuesResult=")
    flatMapValuesResult.collect().foreach{
      it=>print(s"${it},")
    }

    val leftPairRDD = SparkUtils.createRDD(List(("name","jimmy"), ("sex","male"),("age",11)))
    val rightPairRDD = SparkUtils.createRDD(List(("name","Jimmy"),("grade",3)))

    val fullOuterJoinResult = leftPairRDD.fullOuterJoin(rightPairRDD)
    print("\nfullOuterJoinResult=")
    fullOuterJoinResult.collect().foreach {
      it=>print(s"${it},")
    }

    val groupByKeyResult = pairRDD.groupByKey()
    print("\ngroupByKeyResult=")
    groupByKeyResult.collect().foreach{
      it=>print(s"${it},")
    }

    val joinResult = leftPairRDD.join(rightPairRDD)
    print("\njoinResult=")
    joinResult.collect().foreach {
      it=>print(s"${it},")
    }

    val leftOuterJoinResult = leftPairRDD.leftOuterJoin(rightPairRDD)
    print("\nleftOuterJoinResult=")
    leftOuterJoinResult.collect().foreach {
      it=>print(s"${it},")
    }

    val rightOuterJoinResult = leftPairRDD.rightOuterJoin(rightPairRDD)
    print("\nrightOuterJoinResult=")
    rightOuterJoinResult.collect().foreach {
      it=>print(s"${it},")
    }

    val reduceByKeyResult = pairRDD.reduceByKey{
      (a,b)=>a+b
    }
    print("\nreduceByKeyResult=")
    reduceByKeyResult.collect().foreach {
      it=>print(s"${it},")
    }

    /**
      * withReplacement代表不重复抽样
      */
    val data = SparkUtils.createRDD(Array(
      ("female", "Lily"),
      ("female", "Lucy"),
      ("female", "Emily"),
      ("female", "Kate"),
      ("female", "Alice"),
      ("male", "Tom"),
      ("male", "Roy"),
      ("male", "David"),
      ("male", "Frank"),
      ("male", "Jack")))
    val fractions : Map[String, Double]= Map("female"->0.6,"male"->0.4)
    val sampleByKeyResult = data.sampleByKey(false,fractions, 0 )
    print("\nsampleByKeyResult=")
    sampleByKeyResult.collect().foreach {
      it=>print(s"${it},")
    }

    val subtractByKeResult = leftPairRDD.subtractByKey(rightPairRDD)
    print("\nsubtractByKeResult=")
    subtractByKeResult.collect().foreach{
      it=>print(s"${it},")
    }

    val valuesResult = leftPairRDD.values
    print("\nvaluesResult=")
    valuesResult.collect().foreach{
      it=>print(s"${it},")
    }
  }
}
