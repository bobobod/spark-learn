package com.cczu.scala.obj

object EnumDemo {
  def main(args: Array[String]): Unit = {
    println(WorkDay.Monday)
  }
}

// 定义枚举类对象
object WorkDay extends Enumeration {
  val Monday = Value(1, "monday")
}

// 应用类
object TestApp extends App {
  println("hello")
  // 别名
  type MyString = String
  val a: MyString = "abc"
  println(a)
}