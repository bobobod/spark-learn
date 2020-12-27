package com.cczu.spark.rdd.operator.transform

import org.apache.spark.{SparkConf, SparkContext}

/**
 *
 * @author jianzhen.yin
 * @date 2020/12/27
 */
object Oper_Transform_MapPartitions_Par2_Test {
  /**
   * 求每个分区内的数据的最大值
   *
   * @param args
   */
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[*]").setAppName("rdd memory")
    val sc = new SparkContext(conf)
    val rdd = sc.parallelize(List(1, 2, 3, 4),2)
    val mapPartitions = rdd.mapPartitions(iter => {
      List(iter.max).iterator
    })
    mapPartitions.collect().foreach(println)
    sc.stop()
  }
}
