package com.atguigu.bigdata.sparkmall.realtime

import java.util

import com.atguigu.bigdata.sparkmall.common.model.MyKafkaMessage
import com.atguigu.bigdata.sparkmall.common.util.{DateUtil, MyKafkaUtil, RedisUtil}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.SparkConf
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import redis.clients.jedis.Jedis

//需求4： 广告黑名单实时统计
object Req4RedisBlackList {
  def main(args: Array[String]): Unit = {

    // 准备配置对象
    val sparkConf = new SparkConf().setAppName("Req4BlackListApplication").setMaster("local[*]")

    // 构建上下文环境对象
    val ssc = new StreamingContext(sparkConf, Seconds(5))
    ssc.sparkContext.setCheckpointDir("cp")

    // 采集kafka中的数据
    val kafkaDStream: InputDStream[ConsumerRecord[String, String]] = MyKafkaUtil.getKafkaStream("ads_log", ssc)

    val messageDStream: DStream[MyKafkaMessage] = kafkaDStream.map(record => {
      val message = record.value()
      val datas: Array[String] = message.split(" ")
      MyKafkaMessage(datas(0), datas(1), datas(2), datas(3), datas(4))
    })


    // Coding : Driver 1
    val transformDStream: DStream[MyKafkaMessage] = messageDStream.transform(rdd => {
      // Coding : Driver n
      val client: Jedis = RedisUtil.getJedisClient
      val blackList: util.Set[String] = client.smembers("blacklist")
      // Driver => object => Driver
      //blackList.contains("1")
      // 使用广播变量来实现序列化操作
      val blackListBroadcast: Broadcast[util.Set[String]] = ssc.sparkContext.broadcast(blackList)
      client.close()

      rdd.filter(message => {
        // Coding : Executor m
        // Driver ==> Executor
        !blackListBroadcast.value.contains(message.userid)
      })
    })

    // TODO 1. 将数据进行结构的转换 ：(date-adv-user, 1)
    val dateAdvUserToCountDStream: DStream[(String, Long)] = transformDStream.map(message => {
      val date: String = DateUtil.formatTime(message.timestamp.toLong, "yyyy-MM-dd")
      (date + "_" + message.adid + "_" + message.userid, 1L)
    })


    // TODO 2.使用redis对数据进行更新
    dateAdvUserToCountDStream.foreachRDD(rdd=>{
      rdd.foreachPartition(datas=>{
        val jedisClient: Jedis = RedisUtil.getJedisClient
        datas.foreach{
          case (key,sum)=>{
            //向redis中更新数据
            jedisClient.hincrBy("date:adv:userid:click",key,1)
            //获取到redis中最新的数据进行阈值的判断
            val cliclCount: String = jedisClient.hget("date:adv:userid:click",key)
            if (cliclCount.toInt >= 100){
              val keys: Array[String] = key.split("_")
              jedisClient.sadd("blacklist",keys(2))
            }
          }
        }
        jedisClient.close()
      })
    })


    ssc.start()
    ssc.awaitTermination()

  }
}
