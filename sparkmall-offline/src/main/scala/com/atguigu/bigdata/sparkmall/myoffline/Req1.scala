package com.atguigu.bigdata.sparkmall.myoffline

import java.sql.{Connection, DriverManager, PreparedStatement}
import java.util.UUID

import com.atguigu.bigdata.sparkmall.common.model.UserVisitAction
import com.atguigu.bigdata.sparkmall.common.util.{ConfigUtil, StringUtil}
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}
import org.apache.spark.util.AccumulatorV2

import scala.collection.{immutable, mutable}


// TODO 需求1：获取点击、下单和支付数量排名前 10 的品类（top10热门品类）
object Req1 {
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
    UserVisitRDD.foreach(action=>{
      if (action.click_category_id != -1) {
        accumulation.add(action.click_category_id + "-click")
      }else if (action.order_category_ids != null) {
        val ids: Array[String] = action.order_category_ids.split(",")
        for (id <- ids) {
          accumulation.add(id + "-order")
        }
      }else if (action.pay_category_ids != null) {
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

    // TODO 3.将数据进行结构的转换，转换为对象
    val categorieses: immutable.Iterable[top10Categories] = categoryToOpSum.map {
      case (category, map) => {
        top10Categories(UUID.randomUUID().toString, category, map.getOrElse(category + "-click", 0L),
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


    // TODO 5.将数据放入数据库中
    val driver: String = ConfigUtil.getValueFromConfig("jdbc.driver.class")
    val url: String = ConfigUtil.getValueFromConfig("jdbc.url")
    val user: String = ConfigUtil.getValueFromConfig("jdbc.user")
    val password: String = ConfigUtil.getValueFromConfig("jdbc.password")


    Class.forName(driver)

    val connection: Connection = DriverManager.getConnection(url,user,password)
    val sqlString = "insert into category_top10 values(?,?,?,?,?) "
    val statement: PreparedStatement = connection.prepareStatement(sqlString)

    top10.foreach{
      data=>{
        statement.setString(1,data.taskId)
        statement.setString(2, data.category_id)
        statement.setLong(3, data.click_count)
        statement.setLong(4, data.order_count)
        statement.setLong(5, data.pay_count)
        statement.executeUpdate()
      }
    }

    statement.close()
    connection.close()

    session.close()

  }
}

case class top10Categories(taskId:String,category_id:String,click_count:Long,order_count:Long,pay_count:Long)

//声明累加器
class MyAcc extends AccumulatorV2[String,mutable.HashMap[String,Long]] {

  val map = new mutable.HashMap[String,Long]()

  override def isZero: Boolean = map.isEmpty

  override def copy(): AccumulatorV2[String, mutable.HashMap[String, Long]] = new MyAcc

  override def reset(): Unit = map.clear()

  override def add(v: String): Unit = map.getOrElse(v,0L) + 1L

  override def merge(other: AccumulatorV2[String, mutable.HashMap[String, Long]]): Unit = {
    val map1 = map
    val map2 = other.value

    map1.foldLeft(map2){
      ( innerMap,kv )=>{

        //更新map中key的值
        innerMap(kv._1) = innerMap.getOrElse(kv._1,0L) + kv._2

        innerMap
      }
    }
  }

  override def value: mutable.HashMap[String, Long] = map
}