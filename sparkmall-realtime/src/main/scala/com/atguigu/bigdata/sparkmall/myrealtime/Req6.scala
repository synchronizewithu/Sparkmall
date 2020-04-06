package com.atguigu.bigdata.sparkmall.myrealtime

import com.atguigu.bigdata.sparkmall.common.model.MyKafkaMessage
import com.atguigu.bigdata.sparkmall.common.util.{DateUtil, MyKafkaUtil, RedisUtil}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.json4s.jackson.JsonMethods
import redis.clients.jedis.Jedis

//需求6：每天各地区 top3 热门广告
object Req6 {
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

    // TODO 1.将数据进行结构的转换 ( date_area_adv,1L )
    val dateAreaAdvDStream: DStream[(String, Long)] = messageDStream.map {
      message => {
        val date: String = DateUtil.formatTime(message.timestamp.toLong, "yyyy-MM-dd")
        (date + "_" + message.area + "_" + message.adid, 1L)
      }
    }


    // TODO 2.将数据进行聚合  ( date_area_adv,sum )
    val stateDStream: DStream[(String, Long)] = dateAreaAdvDStream.updateStateByKey {
      case (seq, buffer) => {
        val sum: Long = buffer.getOrElse(0L) + seq.sum
        Option(sum)
      }
    }

    // TODO 3.将数据进行转换结构 (date_area,(adv,sum))
    val mapDStream: DStream[(String, (String, Long))] = stateDStream.map {
      case (key, sum) => {
        val keys: Array[String] = key.split("_")

        (keys(0) + "_" + keys(1), (keys(2), sum))
      }
    }


    // TODO 4.将数据进行分组
    val groupDStream: DStream[(String, Iterable[(String, Long)])] = mapDStream.groupByKey()

    // TODO 5.将分组后的数据进行排序
    val sortedDStream: DStream[(String, List[(String, Long)])] = groupDStream.mapValues {
      datas => {
        datas.toList.sortWith {
          (left, right) => {
            left._2 > right._2
          }
        }.take(3)
      }
    }

    // TODO 6.将数据放入redis中 (date_area,(adv,sum))
    sortedDStream.foreachRDD { rdd => {
      rdd.foreachPartition { datas => {

        val jedisClient: Jedis = RedisUtil.getJedisClient

        datas.foreach {
          case (key, value) => {
            val keys: Array[String] = key.split("_")
            val date: String = keys(0)
            val area: String = keys(1)

            import org.json4s.JsonDSL._
            val listJson: String = JsonMethods.compact(JsonMethods.render(value))
            jedisClient.hset("top3_ads_per_day:" + keys(0), area,listJson)
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
