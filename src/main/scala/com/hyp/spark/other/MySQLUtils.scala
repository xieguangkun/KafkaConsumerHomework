package wang.doug.spark.other



import java.sql.{Connection, DriverManager, PreparedStatement}

import scala.io.{BufferedSource, Source}

/**
  * IP地址相关库
  */
object MySQLUtils {


  def save(it: Iterator[(String, Int)]): Unit = {
    //一个迭代器代表一个分区，分区中有多条数据
    //TODO:先获得一个JDBC连接,根据需要修改连接信息
    val conn: Connection = DriverManager.getConnection("jdbc:mysql://localhost:3306/spark?characterEncoding=UTF-8", "root", "root")
    //将数据通过Connection写入到数据库
    val pstm: PreparedStatement = conn.prepareStatement("INSERT INTO area_sum VALUES (?,?,?)")
    //将分区中的数据一条一条写入到MySQL中
    it.foreach(tp => {
      pstm.setInt(1,0)
      pstm.setString(2, tp._1)
      pstm.setInt(3, tp._2)
      pstm.executeUpdate()
    })
    //将分区中的数据全部写完之后，在关闭连接
    if (pstm != null) {
      pstm.close()
    }
    if (conn != null) {
      conn.close()
    }
  }
}
