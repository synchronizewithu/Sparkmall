package com.atguigu.bigdata.sparkmall.test2020

object test {

  def main(args: Array[String]): Unit = {

    val list1: List[Int] = List(1,2,3,4)
    println(list1.zip(list1.tail))
  }
}
