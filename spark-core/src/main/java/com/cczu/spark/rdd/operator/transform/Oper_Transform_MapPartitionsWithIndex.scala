package com.cczu.spark.rdd.operator.transform

import org.apache.spark.{SparkConf, SparkContext}

/**
 *获取第二个分区的数据
 * @author jianzhen.yin
 * @date 2020/12/27
 */
object Oper_Transform_MapPartitionsWithIndex {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[*]").setAppName("rdd memory")
    val sc = new SparkContext(conf)
    val rdd = sc.parallelize(List(1, 2, 3, 4),2)
    val rdd1 = rdd.mapPartitionsWithIndex((index, iter) => {
      if (index == 1) {
        iter
      } else {
        // 空的迭代器
        Nil.iterator
      }
    })
    rdd1.collect().foreach(println)
    sc.stop()
  }
}
