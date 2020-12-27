package com.cczu.spark.rdd.operator.transform

import org.apache.spark.{HashPartitioner, SparkConf, SparkContext}

/**
 *
 * @author jianzhen.yin
 * @date 2020/12/27
 */
object Oper_Transform_PartitionBy {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[*]").setAppName("rdd memory")
    val sc = new SparkContext(conf)
    // 算子 （key-value）类型
    val rdd = sc.makeRDD(List(1, 2, 3, 4),2)
    val newRDD = rdd.map((_, 1))
    // 隐式转换
    // partitionBy根据指定的分区对数据重分区
    // 如果重分区的分区器和当前的分区器是一样的，那么不会改变
    // 默认有三个分区器 Hash 和 range
    newRDD.partitionBy(new HashPartitioner(2)).saveAsTextFile("output")
    sc.stop()
  }
}
