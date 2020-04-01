package com.hyp.streaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.ReceiverInputDStream
import org.apache.spark.streaming.{Seconds, StreamingContext}

object NetTest {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[*]").setAppName("StreamingApp")
    //2s获取一次数据
    val css = new StreamingContext(conf,Seconds(2))
    css.sparkContext.setLogLevel("WARN")
    val wordStream: ReceiverInputDStream[String] = css.socketTextStream("192.168.158.177",9999)
    val words = wordStream.flatMap(_.split(" ")).map(x=>(x,1)).reduceByKey(_+_)

    words.print()

    css.start()
    css.awaitTermination()
  }
}
