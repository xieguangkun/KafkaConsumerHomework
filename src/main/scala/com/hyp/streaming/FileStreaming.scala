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

    /**
      * 时间窗口
      * windowDuration表示的是对过去的一个windowDuration时间间隔的数据进行统计计算， windowDuration是intervalBatch的整数倍，也就是说，假如windowDuration=n*intervalBatch， 那么window操作就是对过去的n个RDD进行统计计算
      * 如下内容来自于Spark Streaming的官方文档：http://spark.apache.org/docs/latest/streaming-programming-guide.html
      *
      * Spark Streaming也提供了窗口计算(window computations)的功能，允许我们每隔一段时间(sliding duration)对过去一个时间段内(window duration)的数据进行转换操作(tranformation).
      * slideDruation控制着窗口计算的频度，windowDuration控制着窗口计算的时间跨度。slideDruation和windowDuration都必须是batchInterval的整数倍。假想如下一种场景：
      * windowDuration=3*batchInterval，
      * slideDuration=10*batchInterval,
      * 表示的含义是每个10个时间间隔对之前的3个RDD进行统计计算，也意味着有7个RDD没在window窗口的统计范围内。slideDuration的默认值是batchInterval
      */
    history_words.reduceByKeyAndWindow((x:Int,y:Int)=>x+y,Seconds(10),Seconds(5))

    scc.start()
    scc.awaitTermination()
  }
}
