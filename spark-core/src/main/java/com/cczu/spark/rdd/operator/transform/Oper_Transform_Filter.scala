package com.cczu.spark.rdd.operator.transform

import org.apache.spark.{SparkConf, SparkContext}

/**
 *
 * @author jianzhen.yin
 * @date 2020/12/27
 */
object Oper_Transform_Filter {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[*]").setAppName("rdd memory")
    val sc = new SparkContext(conf)
    // 将数据根据指定规则过滤，符合规则的保留，
    // 当数据经过筛选后，分区不变，但是分区内的数据可能不均衡，可能会出现数据倾斜问题

    val rdd = sc.parallelize(List(1,2,3,4))
    val flatMapRDD = rdd.filter(_%2 == 0)
    flatMapRDD.collect().foreach(println)
    sc.stop()
  }
}
