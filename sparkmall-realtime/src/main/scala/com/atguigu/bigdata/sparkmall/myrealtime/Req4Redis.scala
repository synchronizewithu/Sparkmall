package com.atguigu.bigdata.sparkmall.myrealtime

import java.util

import com.atguigu.bigdata.sparkmall.common.model.MyKafkaMessage
import com.atguigu.bigdata.sparkmall.common.util.{DateUtil, MyKafkaUtil, RedisUtil}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.SparkConf
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import redis.clients.jedis.Jedis

//需求4：使用redis实现，广告黑名单实时统计
object Req4Redis {
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

    // TODO 1.过滤掉黑名单中的数据
    val transformDStream: DStream[MyKafkaMessage] = messageDStream.transform(rdd => {
      val client: Jedis = RedisUtil.getJedisClient

      val blacklist: util.Set[String] = client.smembers("blacklist")
      val blackListBroadCast: Broadcast[util.Set[String]] = ssc.sparkContext.broadcast(blacklist)

      client.close()

      rdd.filter(message => {
        !blackListBroadCast.value.contains(message.userid)
      })
    })

    // TODO 2.将数据进行结构的转换 (date-adv-user,1L)
    val dateAdvUserToOneDStream: DStream[(String, Long)] = transformDStream.map {
      message => {
        val date: String = DateUtil.formatTime(message.timestamp.toLong, "yyyy-MM-dd")

        (date + "_" + message.adid + "_" + message.userid, 1L)
      }
    }

    // TODO 3.将数据直接放入redis中
    dateAdvUserToOneDStream.foreachRDD(rdd => {
      rdd.foreachPartition { datas => {
        val jedisClient: Jedis = RedisUtil.getJedisClient
        datas.foreach {
          case (key, sum) => {
            //更新值
            jedisClient.hincrBy("date:adv:user:click", key, 1)
            //获取redis中的值
            val clickCount: String = jedisClient.hget("date:adv:user:click", key)
            if (clickCount.toInt >= 100) {
              val keys: Array[String] = key.split("_")
              jedisClient.sadd("blacklist", keys(2))
            }
          }
        }
        jedisClient.close()
      }}
    })


  }
}
