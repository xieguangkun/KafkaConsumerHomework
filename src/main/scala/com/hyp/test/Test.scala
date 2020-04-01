package com.hyp.test

import java.util.Properties

import org.apache.spark.sql.catalyst.expressions.aggregate.Count
import org.apache.spark.sql.{Column, Dataset, Row, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.types.{StructType, StructField, StringType, IntegerType}

//import org.apache.spark.sql.functions._
//df.orderBy(desc("col2")).show
object Test {
  val master = "spark://hadoop01:7077"

  def main(args: Array[String]): Unit = {
    val conf:SparkConf = new SparkConf().setAppName("WordCount")
    val sc:SparkContext = new SparkContext(conf)

    val spark = SparkSession.builder().appName("SparkSessionApp").master("local[*]").getOrCreate()
    val properties = new Properties()
    properties.setProperty("driver","com.mysql.jdbc.Driver")
    properties.setProperty("user","root")
    properties.setProperty("password","root")


    val file:Dataset[Row] = spark.read.format("csv").option("delimiter",",").option("quote","\"").option("escape","\"").option("header",true).load("D:\\QQLive/customers.csv")
    file.createOrReplaceTempView("customer")
//    file.show(10)
    spark.sql("select customer_street,customer_city,count(*) as count from customer group by customer_street,customer_city order by count desc limit 10").show(10)
     //   写入数据库
    //    file.write.mode("overwrite").jdbc("jdbc:mysql://localhost:3306/test","spark",properties)
    //    file.write.mode("overwrite").json("D:\\QQLive/customer.json")
    //    file.groupBy(new Column("customer_street"),new Column("customer_city")).agg(Map("customer_street" -> "count")).withColumnRenamed("count(customer_street)","count").orderBy(new Column("count").desc).show(10)

  }
}
