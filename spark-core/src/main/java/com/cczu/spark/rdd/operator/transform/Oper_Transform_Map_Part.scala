package com.cczu.spark.rdd.operator.transform

import org.apache.spark.{SparkConf, SparkContext}

/**
 *  分区不变性
 * @author jianzhen.yin
 * @date 2020/12/27
 */
object Oper_Transform_Map_Part {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[*]").setAppName("rdd memory")
    val sc = new SparkContext(conf)
    val rdd = sc.parallelize(List(1, 2, 3, 4),2)
    rdd.saveAsTextFile("output")
    val mapRDD = rdd.map(_ * 2)
    mapRDD.saveAsTextFile("output1")
    sc.stop()
  }
}
