package com.cczu.spark.rdd.operator.transform

import org.apache.spark.{SparkConf, SparkContext}

/**
 *
 * @author jianzhen.yin
 * @date 2020/12/27
 */
object Oper_Transform_GroupBy {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[*]").setAppName("rdd memory")
    val sc = new SparkContext(conf)
    val rdd = sc.parallelize(List(1,2,3,4))
    // 分组和分区没有必然的关系
    // groupBy会把数据打乱，还会重新组合 这种操作叫做shuffle，极限情况下，数据可能在同一个分区
    // 一个组的数据在一个分区，但是并不是说一个分区就只有一个组
    val groupByRDD = rdd.groupBy(num => {
      num % 2
    })
    groupByRDD.collect().foreach(println)
    sc.stop()
  }
}
