package com.atguigu.bigdata.sparkmall.myrealtime

import com.atguigu.bigdata.sparkmall.common.model.MyKafkaMessage
import com.atguigu.bigdata.sparkmall.common.util.{DateUtil, MyKafkaUtil}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.dstream.{DStream, InputDStream}

//需求7；统计最近一分钟的广告点击的趋势
object Req7 {
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


    // TODO 1.设定窗口的操作
    val windowDStream: DStream[MyKafkaMessage] = messageDStream.window(Seconds(60), Seconds(10))

    // TODO 2.将数据进行结构的转换（2019-06-22 22：58：22 ==> 2019-06-22 22：50：00）
    val mapDStream: DStream[(String, Long)] = windowDStream.map { message => {
      var time: String = DateUtil.formatTime(message.timestamp.toLong, "yyyy-MM-dd HH:mm:ss")
      time = time.substring(0, time.length - 1) + "0"
      (time, 1L)
    }
    }


    // TODO 3.将转换结构后的数据进行聚合（timeA，sumA）
    val reduceDStream: DStream[(String, Long)] = mapDStream.reduceByKey(_+_)


    // TODO 4.对聚合后的数据进行排序
    val sortedDStream: DStream[(String, Long)] = reduceDStream.transform(rdd => {
      rdd.sortByKey()
    })


    sortedDStream.print()

    ssc.start()
    ssc.awaitTermination()

  }
}
