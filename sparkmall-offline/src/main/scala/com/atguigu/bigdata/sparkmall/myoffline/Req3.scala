package com.atguigu.bigdata.sparkmall.myoffline

import com.atguigu.bigdata.sparkmall.common.model.UserVisitAction
import com.atguigu.bigdata.sparkmall.common.util.{ConfigUtil, StringUtil}
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

//需求3：页面单跳转化率统计
object Req3 {
  def main(args: Array[String]): Unit = {
    // 创建环境
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Req1")
    val session: SparkSession = SparkSession.builder().config(sparkConf).getOrCreate()
    import session.implicits._

    session.sparkContext.setCheckpointDir("cp")

    // TODO 从hive中获取数据
    session.sql("use " + ConfigUtil.getValueFromConfig("hive.database"))

    var sql = "select * from user_visit_action where 1=1"

    val startDate: String = ConfigUtil.getValuefromcondition("startDate")
    val endDate: String = ConfigUtil.getValuefromcondition("endDate")

    if (StringUtil.isNotEmpty(startDate) != null) {
      sql = sql + " and date >= '" + startDate + "'"
    }

    if (StringUtil.isNotEmpty(endDate) != null) {
      sql = sql + " and date <= '" + endDate + "'"
    }

    val df: DataFrame = session.sql(sql)
    val ds: Dataset[UserVisitAction] = df.as[UserVisitAction]
    // 获取到原始数据的RDD
    val UserVisitRDD: RDD[UserVisitAction] = ds.rdd

    //持久化RDD
    UserVisitRDD.checkpoint()


    // TODO 1.求分母
    // TODO 1.1 从原始数据中获取到数据，对数据进行筛选过滤，保留需要统计的页面数据
    val pageids: Array[String] = ConfigUtil.getValuefromcondition("targetPageFlow").split(",")

    //做出这些有效页面的拉链
    val pageid1Topageid2: Array[String] = pageids.zip(pageids.tail).map {
      case (pageid1, pageid2) => {
        (pageid1 + "-" + pageid2)
      }
    }

    // TODO 1.2进行过滤操作
    val filterRDD: RDD[UserVisitAction] = UserVisitRDD.filter { action => {
      pageids.contains(action.page_id.toString)
    }}

    // TODO 1.3将过滤后的数据进行结构的转换
    val pageidToOneRDD: RDD[(Long, Long)] = filterRDD.map(action => {
      (action.page_id, 1L)
    })


    // TODO 1.4将数据进行聚合
    val pageidToSumRDD: RDD[(Long, Long)] = pageidToOneRDD.reduceByKey(_+_)


    //checkpoint延迟计算，只有遇到行动算子，才会计算
    val pageidToSumMap: Map[Long, Long] = pageidToSumRDD.collect().toMap


    // TODO 2.求分子
    // TODO 2.1从hive表中获取原始数据，根据session进行分组（session,Iterator(pageid,action_time)）
    val sessionRDD: RDD[(String, Iterable[UserVisitAction])] = UserVisitRDD.groupBy(action => {
      action.session_id
    })


    // TODO 2.2根据分组后的session对时间进行排序
    val pageidZipRDD: RDD[List[(Long, Long)]] = sessionRDD.map {
      case (session, datas) => {
        //对pageid进行排序
        val actions: List[UserVisitAction] = datas.toList.sortWith {
          (left, rigth) => {
            left.action_time < rigth.action_time
          }
        }

        //只取出pageid进行拉链
        val pageList: List[Long] = actions.map(_.page_id)

        //进行拉链
        val pageidZipList: List[(Long, Long)] = pageList.zip(pageList.tail)
        pageidZipList
      }
    }

    val pageidZipFlatRDD: RDD[(Long, Long)] = pageidZipRDD.flatMap(list=>list)

    // TODO 2.3对拉链后的数据进行过滤
    val pageidsZipRDD: RDD[(Long, Long)] = pageidZipFlatRDD.filter { datas => {
      pageid1Topageid2.contains(datas._1 + "-" + datas._2)
    }
    }


    // TODO 2.4对拉链后的数据进行结构的转换
    val pageidsToOne: RDD[(String, Long)] = pageidsZipRDD.map {
      case (pageid1, pageid2) => {

        (pageid1 + "-" + pageid2, 1L)
      }
    }

    // TODO 2.5 对转换后的数据进行聚合
    val pageidsToSumRDD: RDD[(String, Long)] = pageidsToOne.reduceByKey(_+_)


    // TODO 2.6找到对应的分母的数据，计算转换率
    pageidsToSumRDD.foreach{
      case (pageids,sum)=>{

        //分子：1-2 sum

        //求出分母的key
        val mkey: String = pageids.split("-")(0)

        //分母 1  sum1
        val sum1: Long = pageidToSumMap.getOrElse(mkey.toLong,1L)

        //计算转换率
        println(pageids + "=" + (sum.toDouble/sum1))

      }
    }

    session.close()

  }
}
