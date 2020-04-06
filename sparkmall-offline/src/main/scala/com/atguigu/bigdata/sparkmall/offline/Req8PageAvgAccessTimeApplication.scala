package com.atguigu.bigdata.sparkmall.offline

import com.atguigu.bigdata.sparkmall.common.model.UserVisitAction
import com.atguigu.bigdata.sparkmall.common.util.{ConfigUtil, DateUtil, StringUtil}
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

//需求8:Session步长时长分析（每个页面的平均停留时间）
object Req8PageAvgAccessTimeApplication {
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


    // TODO 1.将数据通过Session进行分组
    val sessionRDD: RDD[(String, Iterable[UserVisitAction])] = userVisitActionRDD.groupBy(action=>action.session_id)

    // TODO 2.将数据进行排序，得到真正的页面的跳转顺序
    val pageFlowRDD: RDD[(String, List[(Long, Long)])] = sessionRDD.mapValues {
      datas => {
        // TODo 2.1 对数据进行排序
        val sortedList: List[UserVisitAction] = datas.toList.sortWith {
          (left, right) => {
            left.action_time < right.action_time
          }
        }

        // TODO 2.2对排好序的数据进行结构的转换  (A,timeA), (B,timeB), (C,timeC)
        val pageToTimeList: List[(Long, String)] = sortedList.map {
          action => {
            (action.page_id, action.action_time)
          }
        }

        // TODO 2.3使用拉链，得到页面的拉链（（pageA，timeA），（pageB，timeB））
        val pageZip: List[((Long, String), (Long, String))] = pageToTimeList.zip(pageToTimeList.tail)

        //TODO 2.4 求出时间差为页面的停留时间（pageA，（timeB - timeA））
        pageZip.map {
          case (page1, page2) => {
            val page1Time: Long = DateUtil.getTimeStamp(page1._2, "yyyy-MM-dd HH:mm:ss")
            val page2Time: Long = DateUtil.getTimeStamp(page2._2, "yyyy-MM-dd HH:mm:ss")

            (page1._1, page2Time - page1Time)
          }
        }

      }
    }

    // TODO 2.5 进行结构转换，只关心时间差
    val mapRDD: RDD[List[(Long, Long)]] = pageFlowRDD.map {
      case (session, list) => list
    }

    // TODO 2.6 将数据进行扁平化操作
    val flatRDD: RDD[(Long, Long)] = mapRDD.flatMap(list=>list)



    // TODO 5.对拉链的数据进行分组（pageA，timeB - timeA），（pageA，timeC - timeB）==> （pageA，Iterator[timediff1，timediff2]）
    val groupByRDD: RDD[(Long, Iterable[Long])] = flatRDD.groupByKey()


    // TODO 6.求出页面的平均停留时间  最终结果(pageA，sum/size)
    groupByRDD.foreach{
      case (pageid,list)=>{

        val avgTime: Long = list.sum / list.size
        println(pageid + "=" + avgTime)
      }
    }


    session.close()

  }
}
