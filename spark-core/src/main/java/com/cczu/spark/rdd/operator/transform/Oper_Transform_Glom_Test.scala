package com.cczu.spark.rdd.operator.transform

import org.apache.spark.{SparkConf, SparkContext}

/**
 * 分区内取最大值并求和
 * @author jianzhen.yin
 * @date 2020/12/27
 */
object Oper_Transform_Glom_Test {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[*]").setAppName("rdd memory")
    val sc = new SparkContext(conf)
    val rdd = sc.parallelize(List(1,2,3,4),2)
    val glomRDD = rdd.glom()
    val maxRDD = glomRDD.map(array => {
      array.max
    })
    println(maxRDD.collect().sum)
    sc.stop()
  }
}
