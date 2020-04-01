package com.hyp.streaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.flume.FlumeUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}

object FlumeStreaming {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[*]").setAppName("StreamingApp")
    val ssc = new StreamingContext(conf,Seconds(2))
    ssc.sparkContext.setLogLevel("WARN")
    val flumeStreaming = FlumeUtils.createStream(ssc,"192.168.158.1",8889)
    flumeStreaming.map(x=>new String(x.event.getBody.array()).trim).flatMap(_.split(" ")).map(x=>(x,1)).reduceByKey(_+_).print()

    ssc.start()
    ssc.awaitTermination()
  }
}
