package com.atguigu.bigdata.sparkmall.realtime

import com.atguigu.bigdata.sparkmall.common.model.MyKafkaMessage
import com.atguigu.bigdata.sparkmall.common.util.{DateUtil, MyKafkaUtil}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}

//需求七：统计最近一分钟的广告点击的趋势
object Req7AdvClickWindowApplication {
  def main(args: Array[String]): Unit = {

    //创建环境
    val sparkConf: SparkConf = new SparkConf().setAppName("Req7AdvClickWindowApplication").setMaster("local[*]")
    val context = new StreamingContext(sparkConf,Seconds(5))

    //采集kafka中的数据
    val kafkaDStream: InputDStream[ConsumerRecord[String, String]] = MyKafkaUtil.getKafkaStream("ads_log",context)

    val messageDStream: DStream[MyKafkaMessage] = kafkaDStream.map(record => {
      val value: String = record.value()
      val keys: Array[String] = value.split(" ")
      MyKafkaMessage(keys(0), keys(1), keys(2), keys(3), keys(4))
    })

    // TODO 1.设定窗口的操作
    val windowDStream: DStream[MyKafkaMessage] = messageDStream.window(Seconds(60),Seconds(10))

    // TODO 2.对数据进行结构的转换（2019-06-22 22：58：22 ==> 2019-06-22 22：50：00）
    val mapDStream: DStream[(String, Long)] = windowDStream.map(message => {
      var time: String = DateUtil.formatTime(message.timestamp.toLong, "yyyy-MM-dd HH:mm:ss")
      var str: String = time.substring(0, time.length - 1) + "0"
      (time, 1L)
    })

    // TODO 3.对转换结构后的数据进行聚合
    val reduceDStream: DStream[(String, Long)] = mapDStream.reduceByKey(_+_)


    // TODO 4.对聚合后的数据进行排序
    val sortedDStream: DStream[(String, Long)] = reduceDStream.transform(rdd => {
      rdd.sortByKey()
    })

    sortedDStream.print()

    context.start()
    context.awaitTermination()
  }
}
