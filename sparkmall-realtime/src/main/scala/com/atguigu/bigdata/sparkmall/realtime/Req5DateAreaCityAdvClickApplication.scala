package com.atguigu.bigdata.sparkmall.realtime

import com.atguigu.bigdata.sparkmall.common.model.MyKafkaMessage
import com.atguigu.bigdata.sparkmall.common.util.{DateUtil, MyKafkaUtil, RedisUtil}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import redis.clients.jedis.Jedis


// 需求5：每天各地区各城市各广告的点击流量实时统计
object Req5DateAreaCityAdvClickApplication {
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

    // TODO 1.将数据进行结构的转换（date_area_city_adv,1）
    val dateAreaCityAdvDStream: DStream[(String, Long)] = messageDStream.map {
      message => {
        val date: String = DateUtil.formatTime(message.timestamp.toLong, "yyyy-MM-dd")
        (date + "_" + message.area + "_" + message.city + "_" + message.adid, 1L)
      }
    }

    // TODO 2.将广告的点击进行聚合  ////  或者直接在redis中进行累加
    val dateAreaCityAdvToCountDStream: DStream[(String, Long)] = dateAreaCityAdvDStream.reduceByKey(_ + _)

    // TODO 3.在redis中进行聚合操作
    dateAreaCityAdvToCountDStream.foreachRDD { rdd => {
      rdd.foreachPartition { datas => {
        val jedisClient: Jedis = RedisUtil.getJedisClient

        datas.foreach {
          case (key, sum) => {
            jedisClient.hincrBy("date:area:city:ads",key, sum)
          }
        }
        jedisClient.close()
      }
      }
    }
    }

    ssc.start()
    ssc.awaitTermination()

  }
}
