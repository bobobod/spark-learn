package com.cczu.spark.rdd

import org.apache.spark.{SparkConf, SparkContext}

/**
 *
 * @author jianzhen.yin
 * @date 2020/12/27
 */
object RDD_Memory {
  def main(args: Array[String]): Unit = {
    // 括号* 表示当前系统最大可用核数 ，不写表示单核
    val conf = new SparkConf().setMaster("local[*]").setAppName("rdd memory")
    val sc = new SparkContext(conf)
    // 从内存中创建RDD，从内存集合创建RDD
    // RDD的作用是封装计算逻辑，并生成task
    val seq = Seq[Int](1, 2, 3, 4)
    // parallelize 并行
    //    val rdd = sc.parallelize(seq)
    // makeRDD底层就是调用parallelize
    val rdd = sc.makeRDD(seq)
    // 调用collect才会真正执行
    rdd.collect().foreach(println)
    sc.stop()
  }
}
