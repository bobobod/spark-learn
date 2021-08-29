package com.cczu.scala.obj

object AbstractClassDemo {
  def main(args: Array[String]): Unit = {
    val personImpl: AbPerson = new AbPersonImpl
    personImpl.eat()
  }
}

abstract class AbPerson {
  // 非抽象属性
  val name: String = "person"
  //抽象属性
  var age: Int

  // 非抽象方法
  def eat(): Unit = {
    println("person eat")
  }

  // 抽象方法
  def sleep(): Unit
}

class AbPersonImpl extends AbPerson {
  var age: Int = 19

  override def eat(): Unit = {
    super.eat()
    println("person eat 2")
  }

  def sleep(): Unit = {
    println("person sleep")
  }
}
