package com.atguigu.bigdata.sparkmall.common.util

object StringUtil {

  /**
    * 判断字符串是否不空
    * @param s
    * @return
    */
  def isNotEmpty(s:String): Boolean ={
      s != null && !"".equals(s)
  }
}
