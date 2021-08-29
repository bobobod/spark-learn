package com.cczu.scala.collection.mutable
import scala.collection.immutable.Queue
import scala.collection.mutable

object QueueDemo {
  def main(args: Array[String]): Unit = {
    val queue: mutable.Queue[String] = new mutable.Queue[String]()
    queue.enqueue("a","b","c")
    println(queue.dequeue())

    // 不可变队列
    val queue2: Queue[String] = Queue("1", "2")
  }

}
