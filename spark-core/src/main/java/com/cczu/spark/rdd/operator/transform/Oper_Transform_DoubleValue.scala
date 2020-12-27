package com.cczu.spark.rdd.operator.transform

import org.apache.spark.{SparkConf, SparkContext}

/**
 *
 * @author jianzhen.yin
 * @date 2020/12/27
 */
object Oper_Transform_DoubleValue {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[*]").setAppName("rdd memory")
    val sc = new SparkContext(conf)
    // 要求两个数据源 数据类型要保持一致，拉链不需要
    // 注意：拉链要求两个数据源的分区数据要保持一致，且数目是一致的
    val rdd1 = sc.makeRDD(List(1, 2, 3, 4))
    val rdd2 = sc.makeRDD(List(4, 5, 3, 4))
    // 交集
    println(rdd1.intersection(rdd2).collect().mkString(","))
    // 并集
    println(rdd1.union(rdd2).collect().mkString(","))
    // 差集
    println(rdd1.subtract(rdd2).collect().mkString(","))
    // 拉链
    println(rdd1.zip(rdd2).collect().mkString(","))

    sc.stop()
  }
}
