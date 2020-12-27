package com.cczu.spark.rdd.operator.transform

import org.apache.spark.{SparkConf, SparkContext}

/**
 *
 * @author jianzhen.yin
 * @date 2020/12/27
 */
object Oper_Transform_Coalesce {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[*]").setAppName("rdd memory")
    val sc = new SparkContext(conf)
    val rdd = sc.parallelize(List(1, 2, 3, 4,5,6,7,8,1),4)

    val coalesceRDD = rdd.coalesce(2)
    coalesceRDD.saveAsTextFile("output")
    sc.stop()
  }
}
