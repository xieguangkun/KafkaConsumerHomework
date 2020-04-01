package wang.doug.spark.other


import scala.io.{BufferedSource, Source}

/**
  * IP地址相关库
  */
object IPUtils {

  /**
    * 将IP地址转换为Long
    *
    * @param ip
    * @return
    */
  def ip2Long(ip: String): Long = {
    val fragments = ip.split("[.]")
    var ipNum = 0L
    for (i <- 0 until fragments.length) {
      ipNum = fragments(i).toLong | ipNum << 8L
    }
    ipNum
  }

  /**
    * 读取IP库
    *
    * @param path
    * @return 记录索引
    */
  def readRules(path: String): Array[(Long, Long, String, String)] = {
    //读取ip规则
    val bf: BufferedSource = Source.fromFile(path)
    val lines: Iterator[String] = bf.getLines()
    //对ip规则进行整理，并放入到内存
    val rules: Array[(Long, Long, String, String)] = lines.map(line => {
      val fileds = line.split("[|]")
      //起始
      val startNum = fileds(2).toLong
      //结束
      val endNum = fileds(3).toLong
      //省
      val province = fileds(6)
      //市
      val city = fileds(7)
      (startNum, endNum, province, city)
    }).toArray
    rules
  }

  /**
    * 二分法查找
    *
    * @param lines
    * @param ip
    * @return
    */
  def binarySearch(lines: Array[(Long, Long, String, String)], ip: Long): Int = {
    var low = 0
    var high = lines.length - 1
    while (low <= high) {
      val middle = (low + high) / 2
      if ((ip >= lines(middle)._1) && (ip <= lines(middle)._2))
        return middle
      if (ip < lines(middle)._1)
        high = middle - 1
      else {
        low = middle + 1
      }
    }
    -1
  }


  def main(args: Array[String]): Unit = {

    //读取IP规则库
    val ipRules: Array[(Long, Long, String, String)] = readRules("D:/QQLive/ip.txt")

    //待查找的IP地址
    val ipNum = ip2Long("61.155.221.227")

    //通过二分法查找，返回索引
    val idx: Int = binarySearch(ipRules, ipNum)


    if (idx == -1) {
      println("没有找到")
    } else {

      //根据索引加载IP规则的元组Rule
      val ipRule: (Long, Long, String, String) = ipRules(idx)
      //输出省、城市
      println(ipRule._3 + "  " + ipRule._4)


    }


  }


}
