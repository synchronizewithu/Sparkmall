package com.atguigu.bigdata.sparkmall.myoffline

import com.atguigu.bigdata.sparkmall.common.model.UserVisitAction
import com.atguigu.bigdata.sparkmall.common.util.{ConfigUtil, DateUtil, StringUtil}
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

//需求8:Session步长时长分析（每个session页面的平均停留时间）
object Req8 {
  def main(args: Array[String]): Unit = {

    //创建环境
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Req2CategoryTop10SessionTop10")
    val session: SparkSession = SparkSession.builder().config(sparkConf).enableHiveSupport().getOrCreate()

    import session.implicits._

    session.sparkContext.setCheckpointDir("cp")

    //TODO 获取用户行为数据
    session.sql("use " + ConfigUtil.getValueFromConfig("hive.database"))

    var sql = "select * from user_visit_action where 1=1"

    val startDate = ConfigUtil.getValuefromcondition("startDate")
    val endDate = ConfigUtil.getValuefromcondition("endDate")

    if (StringUtil.isNotEmpty(startDate)) {
      sql = sql + " and date >= '" + startDate + "'"
    }

    if (StringUtil.isNotEmpty(endDate)) {
      sql = sql + " and date <= '" + endDate + "'"
    }

    val df: DataFrame = session.sql(sql)
    val ds: Dataset[UserVisitAction] = df.as[UserVisitAction]
    val userVisitActionRDD: RDD[UserVisitAction] = ds.rdd


    // TODO 1.根据session先进行分组
    val sessionRDD: RDD[(String, Iterable[UserVisitAction])] = userVisitActionRDD.groupBy(action=>action.session_id)


    // TODO 2.不关心session，需要排序
    val pageFlowRDD: RDD[(String, List[(Long, Long)])] = sessionRDD.mapValues(datas => {
      // TODO 2.1排序
      val actions: List[UserVisitAction] = datas.toList.sortWith {
        (left, right) => {
          left.action_time < right.action_time
        }
      }

      // TODO 2.2只拿出我们关心的数据
      val pageidToTime: List[(Long, String)] = actions.map(action => {
        (action.page_id, action.action_time)
      })

      // TODO 2.3使用拉链
      val pageidTimeZip: List[((Long, String), (Long, String))] = pageidToTime.zip(pageidToTime.tail)

      // TODO 2.4求出页面的停留时间
      pageidTimeZip.map {
        case (page1, page2) => {
          val time1: Long = DateUtil.getTimeStamp(page1._2, "yyyy-MM-dd HH:mm:ss")
          val time2: Long = DateUtil.getTimeStamp(page2._2, "yyyy-MM-dd HH:mm:ss")

          (page1._1, time2 - time1)
        }
      }
    })

    // TODO 3.进行结构的转换，只关心时间差
    val mapRDD: RDD[List[(Long, Long)]] = pageFlowRDD.map {
      case (session, list) => list
    }

    // TODO 4.扁平化
    val flatRDD: RDD[(Long, Long)] = mapRDD.flatMap(list=>list)


    // TODO 5.按照key分组
    val groupRDD: RDD[(Long, Iterable[Long])] = flatRDD.groupByKey()

    // TODO 6.求出页面平均停留时间
    groupRDD.foreach{
      case ( pageid,list )=>{

        println(pageid + "=" + list.sum / list.size)
      }
    }

    session.close()
  }
}
