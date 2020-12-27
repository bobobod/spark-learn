package com.cczu.spark.rdd.operator.transform

import org.apache.spark.{SparkConf, SparkContext}

/**
 *
 * @author jianzhen.yin
 * @date 2020/12/27
 */
object Oper_Transform_SortBy {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[*]").setAppName("rdd memory")
    val sc = new SparkContext(conf)
    // sortBy默认也会shuffle
    val rdd = sc.parallelize(List(1, 7, 4, 3, 5, 6, 7, 8, 1),2)
    rdd.sortBy(num=>num).saveAsTextFile("output")
    sc.stop()
  }
}
