package com.cczu.spark.rdd.operator.transform

import org.apache.spark.{SparkConf, SparkContext}

/**
 *
 * @author jianzhen.yin
 * @date 2020/12/27
 */
object Oper_Transform_Map_Par2 {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[*]").setAppName("rdd memory")
    val sc = new SparkContext(conf)
    // map 算子
    // 1。rdd的计算一个分区内的数据是一个一个执行的
    //    只有前面的数据执行完毕才会执行下一个，所以分区内数据执行是有序的
    // 2。不同分区之间执行是无序的
    val rdd = sc.parallelize(List(1, 2, 3, 4),2)
    val mapRDD = rdd.map(num => {
      println(">>>>>" + num)
      num
    })
    val mapRDD2 = mapRDD.map(num => {
      println("#####" + num)
      num
    })
    mapRDD2.collect()
    sc.stop()
  }
}
