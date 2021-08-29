package com.cczu.scala.obj

object SingletonDemo {
  def main(args: Array[String]): Unit = {

  }
}

class Student002 private(val name: String, val age: Int)

// 饿汉式单例
//object Student002{
//  private val student002 = new Student002("hello",11)
//  def getInstance(): Student002 ={
//    student002
//  }
//}
// 懒汉式单例
object Student002 {
  private var student002: Student002 = _

  def getInstance(): Student002 = {
    if (student002 == null) {
      student002 = new Student002("hello", 11)
    }
    student002
  }
}