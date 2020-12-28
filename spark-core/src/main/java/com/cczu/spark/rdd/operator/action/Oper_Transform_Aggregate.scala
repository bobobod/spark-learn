package com.cczu.spark.rdd.operator.action

import org.apache.spark.{SparkConf, SparkContext}

/**
 *
 * @author jianzhen.yin
 * @date 2020/12/27
 */
object Oper_Transform_Aggregate {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[*]").setAppName("rdd memory")
    val sc = new SparkContext(conf)
    val rdd2 = sc.parallelize(List(4,3,2,1),2)
    // 10 + 14+13+12+11
    // aggregateByKey 的初始值在分区内有效，在分区间无效
    // aggregate 的初始值在分区内和分区间都有效
    println(rdd2.aggregate(10)(_ + _, _ + _))
    sc.stop()
  }
}
