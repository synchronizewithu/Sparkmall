package com.atguigu.bigdata.sparkmall.test2020

import com.atguigu.bigdata.sparkmall.common.model.UserVisitAction
import com.atguigu.bigdata.sparkmall.common.util.{ConfigUtil, ConfigurationUtuils2020, StringUtil}
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

object Req3 {
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

    userVisitActionRDD.checkpoint()
    println(userVisitActionRDD.count())



    // ********************** 需求三 start **********************************

    // todo 获取分母数据    1,2,3,4,5,6,7
    val pageids = ConfigurationUtuils2020.getJsonVal("targetPageFlow").split(",")

    val page1topage2 = pageids.zip(pageids.tail).map {
      case (page1, page2) => {
        (page1 + "-" + page2)
      }
    }


    val filterRDD = userVisitActionRDD.filter {
      action => {
        pageids.contains(action.page_id.toString)
      }
    }

    val pagesumRDD = filterRDD.map {
      action => {
        (action.page_id, 1L)
      }
    }.reduceByKey(_ + _)

    val fenmu: Map[Long, Long] = pagesumRDD.collect().toMap

    // todo 分子
    val sessiongroupRDD: RDD[(String, Iterable[UserVisitAction])] = userVisitActionRDD.groupBy(_.session_id)

    val pageidslist: RDD[List[(Long, Long)]] = sessiongroupRDD.map {
      case (session, datas) => {
        val sortdata = datas.toList.sortWith {
          (left, right) => {
            left.action_time < right.action_time
          }
        }

        val pageidlist: List[Long] = sortdata.map(_.page_id)

        val pageids: List[(Long, Long)] = pageidlist.zip(pageidlist.tail)

        pageids
      }
    }

    val pageidsRDD: RDD[(Long, Long)] = pageidslist.flatMap(list=>list)

    val filterpageid12: RDD[(Long, Long)] = pageidsRDD.filter {
      case (pageid1, pageid2) => {
        page1topage2.contains(pageid1 + "-" + pageid2)
      }
    }

    val fenzi: RDD[(String, Long)] = filterpageid12.map {
      case (page1, page2) => {
        (page1 + "-" + page2, 1L)
      }
    }.reduceByKey(_ + _)

    fenzi.foreach{
      case (pageids , sum ) =>{
        val firstpage = pageids.split("-")(0)

        val sum2 = fenmu.getOrElse(firstpage.toLong,1L)

        sum.toDouble /sum2
      }
    }


    // ********************** 需求三 end   **********************************

    session.close()
  }
}
