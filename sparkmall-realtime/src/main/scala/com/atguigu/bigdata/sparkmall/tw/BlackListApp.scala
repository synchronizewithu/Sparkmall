package com.atguigu.bigdata.sparkmall.tw

import java.util

import com.alibaba.fastjson.JSON
import com.atguigu.bigdata.sparkmall.common.model.MyKafkaMessage
import com.atguigu.bigdata.sparkmall.common.util.{DateUtil, MyKafkaUtil, RedisUtil}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.SparkConf
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import redis.clients.jedis.Jedis

//需求4：广告黑名单实时统计
object BlackListApp {
  def main(args: Array[String]): Unit = {

    //创建环境
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("BlackListApp")
    val ssc = new StreamingContext(sparkConf,Seconds(5))

    // TODO 1.从kafka中获取数据源
    val kafkaDStream: InputDStream[ConsumerRecord[String, String]] = MyKafkaUtil.getKafkaStream("ads_log",ssc)

    // TODO 2.改变数据的格式
    val messageDStream: DStream[MyKafkaMessage] = kafkaDStream.map(record => {
      val keys: Array[String] = record.value().split(" ")
      MyKafkaMessage(keys(0), keys(1), keys(2), keys(3), keys(4))
    })

    messageDStream.print()


    // TODO 3.对数据进行判断，在黑名单中那么就过滤
    val transformDStream: DStream[MyKafkaMessage] = messageDStream.transform(rdd => {

      val jedis: Jedis = RedisUtil.getJedisClient
      val blacklist: util.Set[String] = jedis.smembers("blacklist")
      val blaBroadcast: Broadcast[util.Set[String]] = ssc.sparkContext.broadcast(blacklist)
      jedis.close()
      rdd.filter(message => {
        !blaBroadcast.value.contains(message.userid)
      })
    })


    // TODO 4.对数据进行map(date-adv-user,1)
    val mapDStream: DStream[(String, Long)] = transformDStream.map(message => {
      val date: String = DateUtil.formatTime(message.timestamp.toLong, "yyyy-MM-dd")
      (date + "_" + message.adid + "_" + message.userid, 1L)
    })


    // TODO 5.使用redis对数据进行阈值的判断
    mapDStream.foreachRDD(rdd=>{
      rdd.foreachPartition(datas=>{
        val jedis: Jedis = RedisUtil.getJedisClient
        datas.foreach{
          case (key,sum)=>{
            jedis.hincrBy("date_adv_user:click" ,key,1)

            val count: String = jedis.hget("date_adv_user:click",key)
            if (count.toInt > 100){
              val keys: Array[String] = key.split("_")
              jedis.sadd("blacklist",keys(2))
            }
          }
        }
        jedis.close()
      })
    })



    ssc.start()
    ssc.awaitTermination()
  }
}
