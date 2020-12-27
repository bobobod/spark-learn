package com.cczu.spark.rdd.operator.transform

import org.apache.spark.{SparkConf, SparkContext}

/**
 *
 * @author jianzhen.yin
 * @date 2020/12/27
 */
object Oper_Transform_SortBy1 {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[*]").setAppName("rdd memory")
    val sc = new SparkContext(conf)
    // sortBy默认也会shuffle
    val rdd = sc.parallelize(List(("1",1),("2",2),("11",11)),2)
    // sortBy 方法可以针对指定的规则进行排序，默认是升序
    println(rdd.sortBy(_._1.toInt).collect(),false)
    sc.stop()
  }
}
