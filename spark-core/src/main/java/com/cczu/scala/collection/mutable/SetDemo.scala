package com.cczu.scala.collection.mutable

import scala.collection.mutable

object SetDemo {
  def main(args: Array[String]): Unit = {
    val set: mutable.Set[Int] = mutable.Set(1, 23, 3)
    // 添加元素
    set += 11
    set.add(12)
    set.remove(11)
  }

}
