package com.cczu.spark.acc

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable

/**
 *
 * @author jianzhen.yin
 * @date 2021/1/9
 */
object SparkBC {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local").setAppName("word Count")
    val sc = new SparkContext(conf)
    val rdd1: RDD[(String, Int)] = sc.parallelize(List(("a", 1), ("b", 2), ("c", 3)))
    val rdd2: RDD[(String, Int)] = sc.parallelize(List(("a", 2), ("b", 3), ("c", 4)))
    /*
    join会导致数据几何增长，并且会影响shuffle的性能

    闭包数据，都是以task为单位发送的，每个任务中包含闭包数据。这样可能会导致，一个Executor中包含大量重复数据，并且占用大量的内存

    Executor其实就是一个jvm，所以在启动时，会自动分配内存，所以可以将任务中的闭包数据放置在Executor的内存中，达到共享的作用，

    spark中的广播变量就是把闭包数据放到Executor的内存中
    分布式共享的只读变量

     */
    //    rdd1.join(rdd2).collect()
    //    import scala.collection.JavaConverters._
    val map: mutable.Map[String, Int] = mutable.Map(("a", 2), ("b", 3), ("c", 4))
    val bc: Broadcast[mutable.Map[String, Int]] = sc.broadcast(map)
    rdd1.map {
      case (w, c) => {
        val newVal: Int = bc.value.getOrElse(w, 0)
        (w, (c, newVal))
      }
    }.collect().foreach(println)

    sc.stop()
  }
}
