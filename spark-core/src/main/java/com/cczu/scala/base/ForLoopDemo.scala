package com.cczu.scala.base

import scala.util.control.Breaks
import scala.util.control.Breaks._

object ForLoopDemo {
  def main(args: Array[String]): Unit = {
    for (i <- 1 to 10) {
      println(i)
    }
    println("------")

    // Predef 做了隐式转换
    for (i <- 1.to(10)) {
      println(i)
    }
    println("------")

    // 不包含10
    for (i <- 1 until 10) {
      println(i)
    }
    println("------")

    // 循环守卫
    for (i <- 1 to 10 if i != 5) {
      println(i)
    }
    println("------")

    // 循环步长
    for (i <- 1 to 10 by 2) {
      println(i)
    }
    println("------")
    // 反转
    for (i <- 1 to 10 by 2 reverse) {
      println(i)
    }
    println("------")
    // 循环嵌套
    for (i <- 1 to 3; j <- 1 to 3) {
      println(i + "---" + j)
    }
    // 引入变量
    for (i <- 1 to 3; j = 10 - i) {
      println(i + "---" + j)
    }
    // 循环返回值 默认都是空
    val unit: Unit = for (i <- 1 to 3) {
      "hello world"
    }
    println(unit)
    println("-----")
    // while do..while
    var a: Int = 10
    while (a > 1) {
      println(a)
      a -= 1
    }
    do {
      println(a)
      a += 1
    } while (a > 10)
    println("-----")
    // break 实现异常的抛出和捕捉
    Breaks.breakable(
      for (i <- 0 until 5) {
        if (i == 3) {
          Breaks.break()
        }
      }
    )

    breakable {
      for (i <- 0 until 5) {
        if (i == 3) {
          break()
        }
      }
    }

    // yield
    val arr: Seq[Int] = for (i <- 0 until 10) yield i * i
    println(arr)
  }
}
