package com.cczu.spark.rdd.operator.transform

import org.apache.spark.{SparkConf, SparkContext}

/**
 *
 * @author jianzhen.yin
 * @date 2020/12/27
 */
object Oper_Transform_Req {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[*]").setAppName("rdd memory")
    val sc = new SparkContext(conf)
    // 1. 获取数据源
    val rdd = sc.textFile("data/test4")
    // 2。去除多余数据
    val mapRDD = rdd.map(line => {
      val lines = line.split(" ")
      ((lines(1), lines(4)), 1)
    })
    // 3。对转换后的数据，进行分组聚合
    val reduceByKey = mapRDD.reduceByKey(_ + _)
    // 4。结构转换
    val newMapRDD = reduceByKey.map {
      case ((prv, ad), num) => (prv, (ad, num))
    }
    // 5。转换
    val groupByRDD = newMapRDD.groupByKey()
    // 6。组内排序
    val resultRDD = groupByRDD.mapValues(
      iter => {
        iter.toList.sortBy(_._2)(Ordering.Int.reverse).take(3)
      }
    )
    // 7。获取数据
    resultRDD.collect().foreach(println)
    sc.stop()
  }
}
