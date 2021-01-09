package com.cczu.spark.acc

import org.apache.spark.rdd.RDD
import org.apache.spark.util.LongAccumulator
import org.apache.spark.{SparkConf, SparkContext}

/**
 *
 * @author jianzhen.yin
 * @date 2021/1/9
 */
object SparkAcc {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local").setAppName("word Count")
    val sc = new SparkContext(conf)
    val rdd: RDD[Int] = sc.parallelize(List(1, 2, 3, 4))
    //    val sum: Int = rdd.reduce(_ + _)
    //    println(sum)
    /*
    累加器用来把Executor端的变量信息聚合到Driver端。在Driver端定义好的变量，在Executor端的每个task都会得到这个变量的
    新的副本，每个task更新这些副本信息后，传会Driver端进行合并操作

    累加器是分布式共享只写变量，什么加只写，是因为在不同的Executor中，累加器的值是不能相互访问的

    累加器出现的问题
    少加：转换算子中调用累加器，如果没有行动算子，那么不会执行
    多加：转换算子中调用累加器，如果多次调用行动算子，那么数值会加
     */
    // 获取系统的累加器，spark默认提供了简单的累加器
    val sumAcc: LongAccumulator = sc.longAccumulator("sum")
    //    sc.doubleAccumulator()
    //    sc.collectionAccumulator()
    //    rdd.foreach(num => {
    //      sumAcc.add(num)
    //    })

    val mapRdd: RDD[Int] = rdd.map(num => {
      sumAcc.add(num)
      num
    })
    mapRdd.collect()
    mapRdd.collect()

    println(sumAcc.value)
    sc.stop()
  }

}
