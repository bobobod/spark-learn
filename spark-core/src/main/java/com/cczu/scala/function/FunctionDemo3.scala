package com.cczu.scala.function

object FunctionDemo3 {
  def main(args: Array[String]): Unit = {
    // 1。 闭包使用，scala万物都是对象，函数也是个对象，是存放在堆里头，所以可以实现共享,内部函数可以访问外部函数局部参数
    def add(a: Int): Int => Int = {
      def addB(b: Int): Int = {
        a + b
      }

      addB
    }

    println(add(1)(2))

    // lambda 简化
    def add1(a: Int): Int => Int = a + _

    println(add1(1)(2))

    // 2。 函数柯里化：把一个参数列表的多个参数，变成多个参数列表
    def add2(a: Int)(b: Int): Int = a + b

    println(add2(1)(2))

    // 3。 传名参数,传递的不是一个具体的值，而是一个代码快
    def f1(a: Int): Int = {
      println("hello ")
      a
    }

    def func3(a: => Int): Unit = {
      println(s"a = $a")
      println(s"a = $a")
    }

    func3(f1(11))
    // func3使用了几次a，函数就调用了几次
    //hello
    //a = 11
    //hello
    //a = 11

    // 4。 lazy 懒加载
    val sum: (Int, Int) => Int = (a, b) => {
      println("调用内部")
      a + b
    }
    lazy val result: Int = sum(12, 11)
    println("1.函数调用")
    println(s"2.result = $result")
    println(s"3.result = $result")
  }

}
