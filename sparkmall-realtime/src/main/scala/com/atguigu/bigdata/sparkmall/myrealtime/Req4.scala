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

//需求4：广告黑名单实时统计
object Req4 {
  def main(args: Array[String]): Unit = {
    //创建环境
    val sparkConf: SparkConf = new SparkConf().setAppName("Req4").setMaster("local[*]")
    val context = new StreamingContext(sparkConf, Seconds(5))

    context.sparkContext.setCheckpointDir("cp")

    val kafkaDStream: InputDStream[ConsumerRecord[String, String]] = MyKafkaUtil.getKafkaStream("ads_log", context)
    val messageDStream: DStream[MyKafkaMessage] = kafkaDStream.map {
      record => {
        val value: String = record.value()
        val keys: Array[String] = value.split(" ")
        MyKafkaMessage(keys(0), keys(1), keys(2), keys(3), keys(4))
      }
    }


    // TODO 0.对黑名单中的数据进行过滤--Driver 1次
    val tansformDStream: DStream[MyKafkaMessage] = messageDStream.transform { rdd => {
      // TODO 0.可以进行周期性的检测黑名单--Driver m次
      val client: Jedis = RedisUtil.getJedisClient
      val blacklist: util.Set[String] = client.smembers("blacklist")

      // TODO 0.1可以使用广播变量，实现序列化操作
      val blackListBroadcast: Broadcast[util.Set[String]] = context.sparkContext.broadcast(blacklist)

      client.close()
      rdd.filter {
        // Executor n次
        message => {
          !blackListBroadcast.value.contains(message.userid)
        }
      }
    }
    }

    // TODO 1.将实时数据进行结构的转换 (date-adv-user,1)
    val dateAdvUserToOneDStream: DStream[(String, Long)] = tansformDStream.map(message => {
      val date: String = DateUtil.formatTime(message.timestamp.toLong, "yyyy-MM-dd")
      (date + "_" + message.adid + "_" + message.userid, 1L)
    })

    // TODO 2.将转换结构后的数据进行聚合(date-adv-user,sum)
    val stateDStream: DStream[(String, Long)] = dateAdvUserToOneDStream.updateStateByKey {
      case (seq, buffer) => {
        val sum: Long = buffer.getOrElse(0L) + seq.sum
        Option(sum)
      }
    }

    // TODO 3.将聚合后的数据进行阈值的比对
    stateDStream.foreachRDD(rdd => {
      rdd.foreach {
        case (key, sum) => {
          if (sum >= 100) {
            // TODO 4.如果超过阈值，那么就加入到黑名单中
            val jedisClient: Jedis = RedisUtil.getJedisClient
            val keys: Array[String] = key.split("_")
            jedisClient.sadd("blacklist", keys(2))
            jedisClient.close()
          }
        }
      }
    })


    context.start()
    context.awaitTermination()

  }
}
