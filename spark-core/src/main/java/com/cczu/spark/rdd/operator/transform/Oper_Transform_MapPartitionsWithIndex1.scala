package com.cczu.spark.rdd.operator.transform

import org.apache.spark.{SparkConf, SparkContext}

/**
 * 查询某个数据在那个分区行
 *
 * @author jianzhen.yin
 * @date 2020/12/27
 */
object Oper_Transform_MapPartitionsWithIndex1 {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[*]").setAppName("rdd memory")
    val sc = new SparkContext(conf)
    val rdd = sc.parallelize(List(1, 2, 3, 4),2)
    val rdd1 = rdd.mapPartitionsWithIndex((index, iter) => {
      iter.map((index,_))
    })
    rdd1.collect().foreach(println)
    sc.stop()
  }
}
