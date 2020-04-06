package com.atguigu.bigdata.sparkmall.common.util

import java.io.InputStream
import java.util.{Properties, ResourceBundle}

import com.alibaba.fastjson.{JSON, JSONObject}

object ConfigUtil {

  val rb: ResourceBundle = ResourceBundle.getBundle("config")

  val conditionRb: ResourceBundle = ResourceBundle.getBundle("condition")


  def getValuefromcondition(key:String): String ={
    val conds: String = conditionRb.getString("condition.params.json")
    val jSONObject: JSONObject = JSON.parseObject(conds)
    jSONObject.getString(key)
  }

  /**
    * 从指定的文件中查找指定key的数据
    * @param key
    * @return
    */
  def getValueFromConfig(key:String):String = {
      rb.getString(key)
  }

  /**
    * 从指定的文件中查找指定key的数据
    * @param key
    * @return
    */
  def getValue(key: String): String = {
    val stream: InputStream = Thread.currentThread().getContextClassLoader.getResourceAsStream("config.properties")

    val properties = new Properties()
    properties.load(stream)

    properties.getProperty(key)
  }
}
