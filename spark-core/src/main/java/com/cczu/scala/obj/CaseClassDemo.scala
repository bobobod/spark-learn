package com.cczu.scala.obj

object CaseClassDemo {
  def main(args: Array[String]): Unit = {
    val stu: Student004 = Student004("hhe", 1)
    println(stu.toString)
  }

}

// 定义样例类 伴生对象，apply方法自动生成
case class Student004(name: String, age: Int)