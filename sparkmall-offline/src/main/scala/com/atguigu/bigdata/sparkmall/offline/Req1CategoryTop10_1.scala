package com.atguigu.bigdata.sparkmall.offline

import java.sql.{Connection, DriverManager, PreparedStatement}
import java.util.UUID

import com.atguigu.bigdata.sparkmall.common.model.UserVisitAction
import com.atguigu.bigdata.sparkmall.common.util.{ConfigUtil, StringUtil}
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}
import org.apache.spark.util.AccumulatorV2

import scala.collection.{immutable, mutable}

object Req1CategoryTop10_1 {
  def main(args: Array[String]): Unit = {

    //创建环境
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Req1CategoryTop10")
    val session: SparkSession = SparkSession.builder().config(sparkConf).enableHiveSupport().getOrCreate()

    import session.implicits._


    //TODO 1.从hive中获取用户行为数据
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

    println(userVisitActionRDD.count())

    //TODO 2.使用累加器对不同类别的不同指标的数据进行累加 : (k,v) ==>(category-指标,sum)
    val accumulator = new CategoryActionAccumulator

    session.sparkContext.register(accumulator)

    userVisitActionRDD.foreach {
      action => {
        if (action.click_category_id != -1) {
          accumulator.add(action.click_category_id + "-click")
        } else if (action.order_category_ids != null) {
          val ids: Array[String] = action.order_category_ids.split(",")
          for (id <- ids) {
            accumulator.add(id + "-order")
          }
        } else if (action.pay_category_ids != null) {
          val ids: Array[String] = action.pay_category_ids.split(",")
          for (id <- ids) {
            accumulator.add(id + "-pay")
          }
        }
      }
    }


    //TODO 3.将聚合后的数据进行结构的转换 :(category-指标, SumCount)
    val categoryOpToSum: mutable.HashMap[String, Long] = accumulator.value
//    categoryOpToSum.foreach(println)

    //TODO 4.将数据对key进行分组:(category,(category-指标, SumCount))
    val groupToSum: Map[String, mutable.HashMap[String, Long]] = categoryOpToSum.groupBy {
      case (key, value) => key.split("-")(0)
    }

    //TODO 5.将分组后的数据转换结构，并包装成最终的目标数据
    val top10: immutable.Iterable[CategoryTop10] = groupToSum.map {
      case (category, map) => {
        CategoryTop10(UUID.randomUUID().toString, category,map.getOrElse(category+"-click",0L),
          map.getOrElse(category+"-order",0L),
          map.getOrElse(category+"-pay",0L))
      }
    }

    //TODO 6.将分组后的数据进行排序
    val sorted: List[CategoryTop10] = top10.toList.sortWith {
      (left, right) => {
        if (left.clickCount > right.clickCount) {
          true
        } else if (left.clickCount == right.clickCount) {
          if (left.orderCount > right.orderCount) {
            true
          } else if (left.orderCount == right.orderCount) {
            left.payCount > right.payCount
          }
          false
        } else {
          false
        }
      }
    }

    //TODO 7.取前10个数据
    val categoryTop10: List[CategoryTop10] = sorted.take(10)
    //    categoryTop10.foreach(println)

    //TODO 8.将数据保存到数据库中
    val driver = ConfigUtil.getValueFromConfig("jdbc.driver.class")
    val url = ConfigUtil.getValueFromConfig("jdbc.url")
    val user = ConfigUtil.getValueFromConfig("jdbc.user")
    val password = ConfigUtil.getValueFromConfig("jdbc.password")

    Class.forName(driver)

    val connection: Connection = DriverManager.getConnection(url, user, password)
    val sqlString = "insert into category_top10 values (?,?,?,?,?)"
    val statement: PreparedStatement = connection.prepareStatement(sqlString)

    categoryTop10.foreach(data=>{
      statement.setString(1, data.taskId)
      statement.setString(2, data.categoryId)
      statement.setLong(3, data.clickCount)
      statement.setLong(4, data.orderCount)
      statement.setLong(5, data.payCount)
      statement.executeUpdate()
    })

    statement.close()
    connection.close()

    session.close()

  }
}
