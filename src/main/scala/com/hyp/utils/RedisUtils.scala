package com.hyp.utils

import java.{lang, util}
import java.util.logging.Logger

import org.apache.commons.pool2.impl.GenericObjectPoolConfig
import redis.clients.jedis.{Jedis, JedisPool}

object RedisUtils {
//  private val logger:Logger = new Logger()
  private val redisHost: String = "192.168.158.178"
  private val redisPort: Int = 6379
  private val redisTimeOut: Int = 30000 // ms
  private val maxTotal = 300
  private val maxIdle = 100
  private val minIdle = 1
  private val maxWaitMillis = 10000
  def redisPool():Jedis={
    val poolConfig = new GenericObjectPoolConfig()
    poolConfig.setMaxTotal(maxTotal)
    poolConfig.setMinIdle(minIdle)
    poolConfig.setMaxIdle(maxIdle)
    poolConfig.setTestOnBorrow(true)
    poolConfig.setTestOnReturn(false)
    poolConfig.setMaxWaitMillis(maxWaitMillis)
    new JedisPool(poolConfig, redisHost, redisPort, redisTimeOut).getResource
  }


    def set(key:String,value:String): String ={
      val jedis = redisPool()
      jedis.set(key,value)
    }

    def expire(key:String,time:Int):lang.Long = {
      val jedis = redisPool()
      jedis.expire(key,time)
    }

    def del(key:String)={
      val jedis = redisPool()
      jedis.del(key)
    }

    def get(key:String)={
      val jedis = redisPool()
      jedis.get(key)
    }

    def append(key:String,value:String)={
      val jedis = redisPool()
      jedis.append(key,value)
    }

    def exist(key:String)={
      val jedis = redisPool()
      jedis.exists(key)
    }

  def setnx(key:String, value:String) {
    val jedis = redisPool()
    jedis.setnx(key, value);
  }


  def setex(key:String,seconds:Int, value:String) {
    val jedis = redisPool()
    jedis.setex(key, seconds, value);
  }

  def mget(keys:String*):util.List[String] ={
    val jedis = redisPool()
    var str = ""
    for (key <- keys){
      str = str + " "+key
    }
    jedis.mget(str)
  }

  def mset(keys:String*)={
    val jedis = redisPool()
    var str = ""
    for (key <- keys){
      str = str + " "+key
    }
    jedis.mset(str)
  }

  def hset(key:String,field:String,value:String)={
    val jedis = redisPool()
    jedis.hset(key,field,value)
  }

  def hget(key:String,field:String)={
    val jedis = redisPool()
    jedis.hget(key,field)
  }

  def hgetAll(key:String)={
    val jedis = redisPool()
    jedis.hgetAll(key)
  }

  def main(args: Array[String]): Unit = {
    val pool = redisPool()
    if(pool == null){
//      logger.warning("redis connect error!")
      println("redis connect error!")
    }else{
//      logger.info("redis connect success!")
      println("redis connect success!")
    }
  }
}


