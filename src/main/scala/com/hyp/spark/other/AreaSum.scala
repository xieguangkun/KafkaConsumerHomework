package wang.doug.spark.other

import com.alibaba.fastjson._
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}


/**
  * 根据IP地址汇总分析
  */
object AreaSum {
  /**
    *
    * @param args
    */
  def main(args: Array[String]): Unit = {

    //TODO: 可以通过args(0)传参
    val rulesPath = "D:/QQLive/ip.txt";
    //TODO: 可以通过args(1)传参
    val accesslogFile = "D:/QQLive/sample.txt";

    //local[*]: 这种模式直接帮你按照Cpu最多Cores来设置线程数了
    val conf = new SparkConf().setAppName("AreaSum").setMaster("local[*]")

    val sc = new SparkContext(conf)

    //在Driver端读取到IP规则数据
    val rules: Array[(Long, Long, String, String)] = IPUtils.readRules(rulesPath)

    //将Drive端的IP规则广播到Executor中
    val broadcastRef: Broadcast[Array[(Long, Long, String, String)]] = sc.broadcast(rules)

    //创建RDD，读取访问日志
    val accessLines: RDD[String] = sc.textFile(accesslogFile)


    //转换成（地域，1）的RDD
    val areaAndOne: RDD[(String, Int)] = accessLines.map((line: String) => {

      //每行数据转换为JSON
      val json = JSON.parseObject(line)

      //获取IP地址
      val ipAddr = json.get("clientip").toString

      //将ip转换为数值
      val ipNum = IPUtils.ip2Long(ipAddr)
      //进行二分法查找，通过Driver端的引用或取到Executor中的广播变量，通过广播变量的引用，获取到当前Executor中的广播的规则
      val rulesInExecutor: Array[(Long, Long, String, String)] = broadcastRef.value

      //未匹配
      var area = "未知地域"

      val index = IPUtils.binarySearch(rulesInExecutor, ipNum)
      if (index != -1) {
        //地域： 省+市
        area = rulesInExecutor(index)._3 + rulesInExecutor(index)._4
      }
      //返回元组 (地域，1)
      (area, 1)
    })

    val reduced: RDD[(String, Int)] = areaAndOne.reduceByKey(_ + _)

    //将结果打印,可能存在的瓶颈？
   // val r = reduced.collect()

    //println(r.toBuffer)

   //select * from area_sum where area!='未知地域' order by num desc limit 0,10 ;
    reduced.foreachPartition(it => MySQLUtils.save(it))


    sc.stop()


  }
}
