package com.cczu.scala.collection.immutable

object SetDemo {
  def main(args: Array[String]): Unit = {
    val set: Set[Int] = Set(1, 23, 3)
    val set2: Set[Int] = set + 23
    // 合并set
    val set3: Set[Int] = set ++ set2
    // 删除元素
    val set4: Set[Int] = set - 1

    println(set.size)
  }

}
