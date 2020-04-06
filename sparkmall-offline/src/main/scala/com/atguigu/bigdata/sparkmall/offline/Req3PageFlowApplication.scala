package com.atguigu.bigdata.sparkmall.offline

import java.sql.{Connection, DriverManager, PreparedStatement}
import java.util.UUID

import com.atguigu.bigdata.sparkmall.common.model.UserVisitAction
import com.atguigu.bigdata.sparkmall.common.util.{ConfigUtil, StringUtil}
import org.apache.spark.SparkConf
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

import scala.collection.{immutable, mutable}

//需求3 ：页面单跳转化率统计
object Req3PageFlowApplication {
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

    //RDD重复使用，持久化RDD
    userVisitActionRDD.checkpoint()

    println(userVisitActionRDD.count())

    // ********************** 需求三 start **********************************

    // TODO 1.计算分母
    //1,2,3,4,5,6,7
    //1-2,2-3,3-4,4-5,5-6,6-7
    val pageids: Array[String] = ConfigUtil.getValuefromcondition("targetPageFlow").split(",")

    //使用zip进行拉链
    val pageidsZipArray: Array[String] = pageids.zip(pageids.tail).map {
      case (pageid1, pageid2) => {
        (pageid1 + "-" + pageid2)
      }
    }

    // TODO 1.1 过滤掉不是这些页面的数据
    val filterRDD: RDD[UserVisitAction] = userVisitActionRDD.filter { action => {
      pageids.contains(action.page_id.toString)
    }
    }

    // TODO 1.2 将数据进行结构的转换（pageid, 1）
    val mapRDD: RDD[(Long, Long)] = filterRDD.map { action => {
      (action.page_id, 1L)
    }
    }

    // TODO 1.3 将转换结构后的数据进行聚合(pageid,sum)分母
    val reduceByKeyRDD: RDD[(Long, Long)] = mapRDD.reduceByKey(_+_)

    val pageMap: Map[Long, Long] = reduceByKeyRDD.collect().toMap


    // TODO 2.计算分子
    // TODO 2.1 从行为表中获取数据，并且用session进行分组（sessionid, Iterator[ (pageid, action_time)  ]）
    val sessionRDD: RDD[(String, Iterable[UserVisitAction])] = userVisitActionRDD.groupBy(action => {action.session_id})

    // TODO 2.2 将数据排好顺序，并且将id进行两两结合
    val sortedPageidZip: RDD[List[(Long, Long)]] = sessionRDD.map {
      //排序
      case (session, datas) => {
        val actions: List[UserVisitAction] = datas.toList.sortWith {
          (left, right) => {
            left.action_time < right.action_time
          }
        }

        //取到pageid
        val pageidList: List[Long] = actions.map { action => {
          action.page_id
        }
        }

        //用pageid进行拉链
        val pageidZip: List[(Long, Long)] = pageidList.zip(pageidList.tail)
        pageidZip
      }
    }


    val sortedFlatRDD: RDD[(Long, Long)] = sortedPageidZip.flatMap(list=>list)

    // TODO 2.3 将两两结合的pageids，进行过滤
    val pageidsFilterRDD: RDD[(Long, Long)] = sortedFlatRDD.filter {
      case (pageid1, pageid2) => {
        pageidsZipArray.contains(pageid1 + "-" + pageid2)
      }
    }

    // TODO 2.4 将过滤后的数据转换结构（pageid-pageid2,1）
    val pageidsToOneRDD: RDD[(String, Long)] = pageidsFilterRDD.map {
      pageid => {
        (pageid._1 + "-" + pageid._2, 1L)
      }
    }

    // TODO 2.5 将转换结构后的数据进行聚合
    val pageidsToSumRDD: RDD[(String, Long)] = pageidsToOneRDD.reduceByKey(_+_)


    // TODO 3.计算最后结果
    pageidsToSumRDD.foreach{
      case (pageids,sum) =>{
        //分子 1-2 sum1
        //分母 1 sum2
        val motherNum: String = pageids.split("-")(0)

        val sum2: Long = pageMap.getOrElse(motherNum.toLong,1L)

        println(pageids + "=" + (sum.toDouble /sum2) * 100 + "%")

      }
    }
    // ********************** 需求三 end **********************************

    session.close()

  }
}
