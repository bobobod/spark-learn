package com.cczu.spark.rdd.operator.action

import org.apache.spark.{SparkConf, SparkContext}

/**
 *
 * @author jianzhen.yin
 * @date 2020/12/27
 */
object Oper_Transform_CountByKey {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[*]").setAppName("rdd memory")
    val sc = new SparkContext(conf)
    val rdd2 = sc.parallelize(List(4,3,2,1),2)
    println(rdd2.countByValue())
    val rdd1 = sc.parallelize(List((1,2),(1,3),(2,5)),2)
    println(rdd1.countByKey())
    sc.stop()
  }
}
