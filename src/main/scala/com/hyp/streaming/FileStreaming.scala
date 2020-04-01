package com.hyp.streaming

import org.apache.spark.streaming.flume.FlumeUtils
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.streaming.{Seconds, StreamingContext}

object FileStreaming {
  //历史数据访问
  def updateFunc (inputSum:Seq[Int],resultSum:Option[Int]):Option[Int] = {
    val finalResult:Int = inputSum.sum + resultSum.getOrElse(0)
    Option(finalResult)
  }

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[*]").setAppName("StreamingApp")

    val scc:StreamingContext = new StreamingContext(conf,Seconds(2))
    scc.sparkContext.setLogLevel("WARN")
    //设置检查点目录
    scc.checkpoint("hdfs://192.168.158.177:9000/checkpoint")
    val streamFile = scc.textFileStream("hdfs://192.168.158.177:9000/wordfiles")
    val words = streamFile.flatMap(_.split(" ")).map(x=>(x,1))
//    val lines = FlumeUtils.createStream(scc,"192.168.158.1",8889)
//    lines.map(x=>new String(x.event.getBody.array()).trim).flatMap(_.split(" ")).map(x=>(x,1)).reduceByKey(_+_)
    val history_words = words.updateStateByKey(updateFunc)
    history_words.reduceByKey(_+_)
    history_words.print()
    scc.start()
    scc.awaitTermination()
  }
}
