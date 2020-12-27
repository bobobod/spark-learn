package com.cczu.spark.rdd.operator.transform

import org.apache.spark.{SparkConf, SparkContext}

/**
 *
 * @author jianzhen.yin
 * @date 2020/12/27
 */
object Oper_Transform_FlatMap {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[*]").setAppName("rdd memory")
    val sc = new SparkContext(conf)
    val rdd = sc.parallelize(List(List(1, 2), List(3, 4)))
    val flatMapRDD = rdd.flatMap(list=>list)
    flatMapRDD.collect().foreach(println)
    sc.stop()
  }
}
