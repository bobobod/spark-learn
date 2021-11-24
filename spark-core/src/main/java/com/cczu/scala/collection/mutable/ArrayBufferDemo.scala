package com.cczu.scala.collection.mutable

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

object ArrayBufferDemo {
  def main(args: Array[String]): Unit = {
    // 创建可变数组
    val buffer: ArrayBuffer[Int] = new ArrayBuffer[Int]()
    val buffer2 = ArrayBuffer(1,2,3,4,5)
    println(buffer2)
    buffer2(1) = 10
    // 添加元素 和不可变数组不大一样，不会产生新数组 往后面添加元素
    buffer2 += 18
    buffer2.append(123)
    // 往前面添加元素
    77 +=: buffer2
    buffer2.prepend(111,112)
    println(buffer2)

    // 某个位置添加
    buffer2.insert(1,12,34)
    println(buffer2)

    // 删除元素 删除某个索引的数据
    buffer2.remove(3)
    // 按照元素删除
    buffer2 -= 1111

    // 可变数组转换成不可变
    val arr: Array[Int] = buffer2.toArray
    // 不可变变可变
    val buff: mutable.Buffer[Int] = arr.toBuffer
    val map: Map[String, String] = Map("a" -> "1", "b" -> "2")
    println(map.get("c").toString)
  }

}
