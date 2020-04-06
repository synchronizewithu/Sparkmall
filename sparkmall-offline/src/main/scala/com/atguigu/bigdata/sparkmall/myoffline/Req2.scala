package com.atguigu.bigdata.sparkmall.myoffline

import java.sql.{Connection, DriverManager, PreparedStatement}
import java.util.UUID

import com.atguigu.bigdata.sparkmall.common.model.UserVisitAction
import com.atguigu.bigdata.sparkmall.common.util.{ConfigUtil, StringUtil}
import org.apache.spark.SparkConf
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

import scala.collection.{immutable, mutable}

//需求2：Top10 热门品类中 Top10 活跃 Session 统计
object Req2 {
  def main(args: Array[String]): Unit = {
    // 创建环境
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Req1")
    val session: SparkSession = SparkSession.builder().config(sparkConf).getOrCreate()
    import session.implicits._

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

    // TODO 0.将声明好的累加器进行创建和注册
    val accumulation = new MyAcc
    session.sparkContext.register(accumulation)

    // TODO 1.使用累加器对不同品类的不同指标的数据进行累加(category-指标，sum)
    UserVisitRDD.foreach(action => {
      if (action.click_category_id != -1) {
        accumulation.add(action.click_category_id + "-click")
      } else if (action.order_category_ids != null) {
        val ids: Array[String] = action.order_category_ids.split(",")
        for (id <- ids) {
          accumulation.add(id + "-order")
        }
      } else if (action.pay_category_ids != null) {
        val ids: Array[String] = action.pay_category_ids.split(",")
        for (id <- ids) {
          accumulation.add(id + "-pay")
        }
      }
    })


    // TODO 2.对累加后的结果按照category进行分组 （category-click,100）(category-order,40) -->(category,(指标，sum))
    val categoryOpToSum: mutable.HashMap[String, Long] = accumulation.value

    val categoryToOpSum: Map[String, mutable.HashMap[String, Long]] = categoryOpToSum.groupBy {
      case (categoryX, sum) => {
        val keys: Array[String] = categoryX.split("-")
        keys(0)
      }
    }
    val taskId: String = UUID.randomUUID().toString


    // TODO 3.将数据进行结构的转换，转换为对象
    val categorieses: immutable.Iterable[top10Categories] = categoryToOpSum.map {
      case (category, map) => {
        top10Categories(taskId, category, map.getOrElse(category + "-click", 0L),
          map.getOrElse(category + "-order", 0L),
          map.getOrElse(category + "-pay", 0L))
      }
    }

    // TODO 3.对分组后的数据进行排序（降序）
    val sortedList: List[top10Categories] = categorieses.toList.sortWith {
      (left, right) => {
        if (left.click_count > right.click_count) {
          true
        } else if (left.click_count == right.click_count) {
          if (left.order_count > right.order_count) {
            true
          } else if (left.order_count == right.order_count) {
            left.pay_count > right.pay_count
          }
          false
        }
        false
      }
    }

    // TODO 4.取出top10的品类
    val top10: List[top10Categories] = sortedList.take(10)

    val ids: List[String] = top10.map(_.category_id)
    val idsBroadCast: Broadcast[List[String]] = session.sparkContext.broadcast(ids)

    //****************************** 需求2 start ***************************************
    // TODO 1.根据需求1中的结果对原始数据进行过滤,
    // TODO 注意：在算子内进行的操作，top10集合应该使用广播变量
    val filterRDD: RDD[UserVisitAction] = UserVisitRDD.filter { action => {
      if (action.click_category_id != -1) {
        idsBroadCast.value.contains(action.click_category_id.toString)
      } else {
        false
      }
    }
    }

    // TODO 2.将过滤后的数据进行结构的转换（category-session,1）
    val categorySessionToOneRDD: RDD[(String, Long)] = filterRDD.map { action => {
      (action.click_category_id + "-" + action.session_id, 1L)
    }
    }


    // TODO 3.将数据进行聚合（category-session,sum）
    val categorySessionToSumRDD: RDD[(String, Long)] = categorySessionToOneRDD.reduceByKey(_ + _)

    // TODO 4.将聚合后的数据进行结构的转换 (category-session,sum） -->>（category,(session,sum)）
    val categoryToSessionSumRDD: RDD[(String, (String, Long))] = categorySessionToSumRDD.map {
      case (categoryX, sum) => {
        val ids: Array[String] = categoryX.split("-")

        (ids(0), (ids(1), sum))
      }
    }


    // TODO 5.将转换后的数据进行分组（category,Iterator(session,sum)）
    val categoryToListRDD: RDD[(String, Iterable[(String, Long)])] = categoryToSessionSumRDD.groupByKey()

    // TODO 6.将分组后的数据进行排序（降序），取前10
    val top10RDD: RDD[(String, List[(String, Long)])] = categoryToListRDD.mapValues { datas =>
      datas.toList.sortWith {
        (left, rigth) => {
          left._2 > rigth._2
        }
      }.take(10)
    }

    // TODO 6.1 将结果封装成对象
    val listRDD: RDD[List[top10CategoryTop10Session]] = top10RDD.map {
      case (category, list) => {
        list.map {
          case (session, sum) => {
            top10CategoryTop10Session(taskId, category, session, sum)
          }
        }
      }
    }

    val resultRDD: RDD[top10CategoryTop10Session] = listRDD.flatMap(list => list)

    // TODO 7.将结果保存到数据库中
    resultRDD.foreachPartition { datas => {
      val driver = ConfigUtil.getValueFromConfig("jdbc.driver.class")
      val url = ConfigUtil.getValueFromConfig("jdbc.url")
      val user = ConfigUtil.getValueFromConfig("jdbc.user")
      val password = ConfigUtil.getValueFromConfig("jdbc.password")

      Class.forName(driver)

      val connection: Connection = DriverManager.getConnection(url,user,password)

      val sqlString = "insert into category_top10_session_count values(?,?,?,?)"
      val statement: PreparedStatement = connection.prepareStatement(sqlString)

      datas.foreach{
        data=>{
          statement.setString(1,data.taskID)
          statement.setString(2,data.categoryId)
          statement.setString(3,data.sessionId)
          statement.setLong(4,data.clickCount)
          statement.executeQuery()
        }
      }


      statement.close()
      connection.close()

    }}

    //******************************** 需求2 end *************************************


    session.close()

  }
}


case class top10CategoryTop10Session(taskID: String, categoryId: String, sessionId: String, clickCount: Long)