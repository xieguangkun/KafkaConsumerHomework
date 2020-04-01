package com.hyp.sparkmain

import org.apache.spark.{SparkConf, SparkContext}

object WordCount {
  def main(args: Array[String]): Unit = {
    val conf:SparkConf = new SparkConf().setAppName("WordCount")
    val sc:SparkContext = new SparkContext(conf)


    val lines = sc.textFile(args(0))
    val results = lines.flatMap(lines=>lines.split(" ")).map(x=>(x,1)).reduceByKey(_+_)
    //output
    results.saveAsTextFile(args(1))
    sc.stop()
  }
}
