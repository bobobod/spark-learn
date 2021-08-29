package com.cczu.scala.base


object ImplicitDemo {

  implicit def convert(a: A): MyHandle = {
    new MyHandle
  }

  implicit val str: String = "hello world value"

  def main(args: Array[String]): Unit = {
    // 1. 隐式函数，动态给类添加方法
    val a = new A
    a.printGG()
    // 2. 隐式值
    // 2.1 同一个作用域下，相同类型的隐式值只有一个（只与匹配类型有关，和函数名无关） 2.2 隐式参数高于默认参数
    hello
    // 3. 隐式类
    1.print()
  }

  implicit class B(val self: Int) {
    def print():Unit={
      println(self)
    }
  }

  def hello(implicit arg: String = "good bey"): Unit = {
    println(arg)
  }

}

class A {}

class MyHandle {
  def printGG(): Unit = {
    println("hello world")
  }
}
