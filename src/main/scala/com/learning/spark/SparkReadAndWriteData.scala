package com.learning.spark
import java.io.{StringReader, StringWriter}
import java.util

import com.opencsv.CSVReader
import org.apache.hadoop.mapred.{SequenceFileInputFormat, SequenceFileOutputFormat}
import org.apache.hadoop.io.{IntWritable, Text}

object SparkReadAndWriteData {

  def main(args: Array[String]): Unit = {
    println("------------------read json-----------------")
    var cvsRDD = SparkUtils.createRDD("./iris.data.txt").foreach {
      line=>
        val reader = new CSVReader(new StringReader(line), ',')
        println(util.Arrays.toString((reader.readNext().asInstanceOf[Array[AnyRef]])))
    }

    println("------------------save sequence-----------------")
    val saveSequenceRDD = SparkUtils.createRDD(List(("Panda",3), ("Kay", 6), ("Snail", 2)))
    saveSequenceRDD.saveAsSequenceFile("sequence_file")

    println("------------------read sequence-----------------")
    val readSequenceRDD = SparkUtils.readSequenceFile("./sequence_file/part-00000", classOf[Text], classOf[IntWritable]).map{
      case(x,y)=>(x.toString, y.get())
    }
    readSequenceRDD.collect().foreach {
      case (k,v)=>print(s"(${k},${v},)")
    }

    println("\n------------------read by old hadoop api-----------------")
    val oldHadoopApiRDD = SparkUtils.readFileByOldHadoopApi[Text, IntWritable, SequenceFileInputFormat[Text, IntWritable]]("./sequence_file/part-00000").map{
      case(x,y)=>(x.toString, y.get())
    }
    oldHadoopApiRDD.collect().foreach {
      case (k,v)=>print(s"(${k},${v}),")
    }

    println("\n------------------read by new hadoop api-----------------")
    val newHadoopApiRDD = SparkUtils.readFileByNewHadoopApi[Text, IntWritable, org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat[Text, IntWritable]]("./sequence_file/part-00000", classOf[org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat[Text, IntWritable]], classOf[Text], classOf[IntWritable]).map{
      case(x,y)=>(x.toString, y.get())
    }
    newHadoopApiRDD.collect().foreach {
      case (k,v)=>print(s"(${k},${v}),")
    }

    println("\n------------------read json by HiveContext-----------------")
    val hiveContext = SparkUtils.createHiveContext()
    val dataFrame = hiveContext.jsonFile("./tweets.json")
    dataFrame.registerTempTable("dataFrame")
    val selectDataFrame = hiveContext.sql("select user.name, text from dataFrame")
    selectDataFrame.rdd.collect().foreach{
      it=>print(s"${it},")
    }

    println("\n------------------save by old hadoop api-----------------")
    oldHadoopApiRDD.map{
      case(x,y)=>(new Text(x), new IntWritable(y))
    }.saveAsHadoopFile[SequenceFileOutputFormat[Text, IntWritable]]("./old_save_hadoop_file")

    println("\n------------------save by new hadoop api-----------------")
    newHadoopApiRDD.map {
      case(x,y)=>(new Text(x),new IntWritable(y))
    }.saveAsNewAPIHadoopFile[org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat[Text, IntWritable]]("./new_save_hadoop_file")
  }
}
