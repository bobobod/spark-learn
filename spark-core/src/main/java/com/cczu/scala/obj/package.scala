package com.cczu.scala

package object obj {
  // 当前包共享的属性和方法
  val commonValue = "hello world"

  def commonMethod(): Unit = {
    println("common method¬")
  }
}
