package com.atguigu.bigdata.sparkmall.common.util

import java.io.InputStream
import java.util.{Properties, ResourceBundle}

import com.alibaba.fastjson.{JSON, JSONObject}

object ConfigurationUtuils2020 {

  private val rd: ResourceBundle = ResourceBundle.getBundle("config")

  private val conds: ResourceBundle = ResourceBundle.getBundle("condition")

  def main(args:Array[String]): Unit ={
    val str = getValue("hive.database")
    println(str)

    val str1 = getvalByclassloader("hive.database")
    println(str1)

    println(getvalWithRD("hive.database"))

  }

  //读取Json数据
  def getJsonVal(key:String): String ={
    val value = conds.getString("condition.params.json")
    val jsonObj: JSONObject = JSON.parseObject(value)
    jsonObj.getString(key)
  }

  //使用resourceBundle
  def getvalWithRD(key:String): String ={
    rd.getString(key)
  }

  //使用classloader
  def getValue(key:String): String ={
    val stream: InputStream = Thread.currentThread().getContextClassLoader.getResourceAsStream("config.properties")

    val properties = new Properties()
    properties.load(stream)

    properties.getProperty(key)
  }

  def getvalByclassloader(key:String): String ={
    val stream = ConfigurationUtuils2020.getClass.getClassLoader.getResourceAsStream("config.properties")

    val properties = new Properties()
    properties.load(stream)

    properties.getProperty(key)
  }


}
