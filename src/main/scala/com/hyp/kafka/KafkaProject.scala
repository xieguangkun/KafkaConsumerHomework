package com.hyp.kafka

import java.util
import java.util.logging.Logger

import com.hyp.utils.RedisUtils
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka010.{CanCommitOffsets, ConsumerStrategies, ConsumerStrategy, HasOffsetRanges, KafkaUtils, LocationStrategies, LocationStrategy, OffsetRange}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.kafka.common.serialization.StringDeserializer

import scala.collection.JavaConverters
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import redis.clients.jedis.Jedis
import sun.security.acl.GroupImpl

import scala.collection.mutable

object KafkaProject {
//  val logger:Logger  = new Logger()
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setMaster("local[2]").setAppName("KafkaApp")
    val context = new SparkContext(conf)
    context.setLogLevel("WARN")

    val ssc = new StreamingContext(context , Seconds(5))

    val locationStrategy: LocationStrategy = LocationStrategies.PreferConsistent

    val brokers = "storm01:9092"
    val topic = "test"
    val group = "sparkGroup"

    val kafkaParam = Map(
      "bootstrap.servers"-> brokers,
      "key.deserializer" ->classOf[StringDeserializer],
      "value.deserializer"->classOf[StringDeserializer],
      "group.id"->group,
      "auto.offset.reset"-> "latest",
      "enable.auto.commit" ->(false:java.lang.Boolean)
    );

//    val clientOffset:String = group +"::" +topic
//    val offset = RedisUtils.get(clientOffset)

    val resultDStream: InputDStream[ConsumerRecord[String, String]] = createStreamingContextOrRedis(ssc,locationStrategy,Array(topic),kafkaParam)

    resultDStream.foreachRDD(iter=>{
      if(iter.count() > 0 ){
        iter.foreach(record =>{
            //这里可以直接获取record.offset(),record.topic(),若想根据分区来存储用partitionForEach，根据key为groupId+topic+partition,offset存入redis效果较好
            val value : String = record.value()
            println("offset: "+record.offset()+" value:"+value)
        })
        //ranges里先遍历，可以获取range.topic和rang.untilOffset,记得传一个groupId
        val ranges: Array[OffsetRange] = iter.asInstanceOf[HasOffsetRanges].offsetRanges
        //双重保障
        saveOffset2Redis(ranges,group)
        resultDStream.asInstanceOf[CanCommitOffsets].commitAsync(ranges)
      }

    })

    ssc.start()
    ssc.awaitTermination()
  }

  /**
    * 存入redis中
    * @param ranges
    * @param groupId
    */
  def saveOffset2Redis(ranges: Array[OffsetRange], groupId: String):Unit={
    for( range <- ranges){
      val key = groupId+"::"+range.topic
      val field = groupId+"::"+range.topic+"::"+range.partition
      val value = range.untilOffset
      RedisUtils.hset(key,field,value.toString)
    }
  }

  /**
    * 根据topic和groupId获取offset
    * @param topics
    * @param groupId
    * @return
    */
  def getOffset(topics:Array[String],groupId:String): (mutable.Map[TopicPartition, Long],Int) ={
    val offsets = scala.collection.mutable.Map[TopicPartition,Long]()
    topics.foreach(topic =>{
      val keys: util.Map[String, String] = RedisUtils.hgetAll(groupId.trim+"::"+topic.trim)
      val map = JavaConverters.mapAsScalaMapConverter(keys).asScala
      if(map != null){
        for((field,value) <- map){
          val partitionField = field
          val offset = value
          //map为group+topic,获取该map下的所有值,field按照::切分之后第三个为partition值,offset就是数据
          println(partitionField.split("::")(2).toInt)
          offsets.put(new TopicPartition(topic,partitionField.split("::")(2).toInt),offset.toLong)
        }
      }
    })
    if (offsets.isEmpty) {
      (offsets, 0)
    } else {
      (offsets, 1)
    }
  }

  /**
    * 查询redis中有没有offset,没有就重新获取
    * @param ssc
    * @param locationStrategy
    * @param topic
    * @param kafkaParams
    * @return
    */
  def createStreamingContextOrRedis(ssc: StreamingContext, locationStrategy:LocationStrategy, topic: Array[String],
                                  kafkaParams: Map[String, Object]): InputDStream[ConsumerRecord[String, String]] = {
    var kafkaStreams: InputDStream[ConsumerRecord[String, String]] = null
    val groupId = kafkaParams.get("group.id").get
    val (offsets, flag) = getOffset(topic, groupId.toString)
    val offsetReset = kafkaParams.get("auto.offset.reset").get
    var consumerStrategy: ConsumerStrategy[String, String] = null
    if (flag == 1 && offsetReset.equals("latest")) {
      println("redis中")
      //这个第三个offset参数传递一个TopicPartion类，里面存放topic和partition获取指定的位置，从后面的long值也就是offset值获取到offset位置
      consumerStrategy = ConsumerStrategies.Subscribe(topic, kafkaParams,offsets)
      kafkaStreams = KafkaUtils.createDirectStream(ssc, locationStrategy,
        consumerStrategy)
    } else {
      println("自己创建")
      consumerStrategy = ConsumerStrategies.Subscribe(topic, kafkaParams)
      kafkaStreams = KafkaUtils.createDirectStream(ssc, locationStrategy,
        consumerStrategy)
    }
    kafkaStreams
  }
}
