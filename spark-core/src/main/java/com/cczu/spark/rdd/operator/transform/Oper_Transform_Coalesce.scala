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
    val rdd = sc.parallelize(List(1, 2, 3, 4, 5, 6, 7, 8, 1), 4)
    // coalesce 默认不会打乱数据
    // 可能会导致数据不均衡，数据倾斜
    // 可以重新进行shuffle
    //    val coalesceRDD = rdd.coalesce(2)
    //    coalesceRDD.saveAsTextFile("output")
    // 为true会进行shuffle
    // coalesce 算子可以扩大分区，但是如果不shuffle操作，是没有意义的，不起作用

    // 缩减分区：coalesce ,如果想要数据均衡，可以采用shuffle
    // 扩大分区：repartition,底层默认使用coalesce且使用true
    //    rdd.coalesce(4,true)
    val coalesce = rdd.coalesce(2, true)
    coalesce.saveAsTextFile("output")
    sc.stop()
  }
}
