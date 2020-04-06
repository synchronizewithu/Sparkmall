package com.atguigu.bigdata.sparkmall.common.util

import java.text.SimpleDateFormat
import java.util.Date

object DateUtil {

  /**
    * 将指定的时间戳转换为时间字符串
    * @param ts
    * @param format
    * @return
    */
  def formatTime(ts:Long,format:String): String ={
    formatDate(new Date(ts),format)
  }


  def formatDate(date:Date,format:String): String ={
    val sdf = new SimpleDateFormat(format)
    sdf.format(date)
  }


  /**
    * 将时间字符串按照指定的格式转换为时间戳
    * @param dateString
    * @param format
    * @return
    */
  def getTimeStamp(dateString:String,format:String): Long ={
    val sdf = new SimpleDateFormat(format)
    sdf.parse(dateString).getTime
  }
}
