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

object Req1CategoryTop10 {
  def main(args: Array[String]): Unit = {

    //创建环境
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Req1CategoryTop10")
    val session: SparkSession = SparkSession.builder().config(sparkConf).enableHiveSupport().getOrCreate()

    import session.implicits._
    /**
      * 需求1：获取点击，下单和支付数量前10的品类
      *
      * 1.从hive表中获取数据
      * 2.使用累加器将不同的类型的不同指标的数据进行累加，HashMap（k,v）==>( category-指标，sum )
      * 3.将数据进行结构的转换 ( category-指标，sum ) ==>  ( category,(指标，sum) )
      * 4.将相同的key分组在一起
      * 5.将品类的不同的指标进行排序（降序）
      * 6.取前十的品类
      *
      */

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


    //TODO 3.将聚合后的数据进行结构的转换 :(category-指标, SumCount) (category,(指标, SumCount))
    val categoryOpToSum: mutable.HashMap[String, Long] = accumulator.value
    val categoryToOpsum: mutable.HashMap[String, (String, Long)] = categoryOpToSum.map {
      case (keys, sum) => {
        val key: Array[String] = keys.split("-")
        (key(0), (key(1), sum))
      }
    }

    //TODO 4.将数据对key进行分组
    val groupToSum: Map[String, mutable.HashMap[String, (String, Long)]] = categoryToOpsum.groupBy {
      case (key, value) => key
    }

    //(12,Map(12 -> (click,50)))
    //(12,Map(12 -> (order,30)))
    //(12,Map(12 -> (pay,20)))
    //    groupToSum.foreach(println)

    //TODO 5.将分组后的数据转换结构，并包装成最终的目标数据
    val top10: immutable.Iterable[CategoryTop10] = groupToSum.map {
      case (category, map) => {
        val maybeTuple: Option[(String, Long)] = map.get(category)
        val t: (String, Long) = maybeTuple.get

        var clickCount = 0L
        var orderCount = 0L
        var payCount = 0L

        if ("click".equals(t._1)) {
          clickCount = t._2
        } else if ("order".equals(t._1)) {
          orderCount = t._2
        } else {
          payCount = t._2
        }
        CategoryTop10(UUID.randomUUID().toString, category, clickCount, orderCount, payCount)
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
    categoryTop10.foreach(println)


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

case class CategoryTop10(taskId: String, categoryId: String, clickCount: Long, orderCount: Long, payCount: Long)

//声明累加器 （k,v）==>(category-指标，sum)
class CategoryActionAccumulator extends AccumulatorV2[String, mutable.HashMap[String, Long]] {

  var map = new mutable.HashMap[String, Long]()

  override def isZero: Boolean = {
    map.isEmpty
  }

  override def copy(): AccumulatorV2[String, mutable.HashMap[String, Long]] = {
    new CategoryActionAccumulator
  }

  override def reset(): Unit = {
    map.clear()
  }

  override def add(v: String): Unit = {
    map(v) = map.getOrElse(v, 0L) + 1L
  }

  override def merge(other: AccumulatorV2[String, mutable.HashMap[String, Long]]): Unit = {
    var map1 = map
    var map2 = other.value

    map = map1.foldLeft(map2) {
      (innerMap, kv) => {
        innerMap(kv._1) = innerMap.getOrElse(kv._1, 0L) + kv._2
        innerMap
      }
    }

  }

  override def value: mutable.HashMap[String, Long] = map
}