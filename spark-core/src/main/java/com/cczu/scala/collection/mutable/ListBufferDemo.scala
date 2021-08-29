package com.cczu.scala.collection.mutable

import scala.collection.mutable
import scala.collection.mutable.{ArrayBuffer, ListBuffer}

object ListBufferDemo {
  def main(args: Array[String]): Unit = {
    val buffer: ListBuffer[Int] = new ListBuffer[Int]()
    val buffer2: ListBuffer[Int] = ListBuffer(1, 23, 3)
    buffer.append(111)
    buffer.prepend(222)
    buffer2 += 18
    // 往前面添加元素
    77 +=: buffer2
    // 合并
    val buffer3: ListBuffer[Int] = buffer ++ buffer2
    buffer2.update(0,1)

  }

}
