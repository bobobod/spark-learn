package com.cczu.spark.rdd

import org.apache.spark.{SparkConf, SparkContext}

/**
 * rdd 分区 数据分布
 *
 * @author jianzhen.yin
 * @date 2020/12/27
 */
object RDD_Memory_Par1 {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[*]").setAppName("rdd memory")
    val sc = new SparkContext(conf)
    // [1,2] [3,4]
    //    val rdd = sc.makeRDD(List(1, 2, 3, 4),2)
    // [1】,[2],[3,4]
    //    val rdd = sc.makeRDD(List(1, 2, 3, 4), 3)
    // [1],[2,3],[4,5]
    val rdd = sc.makeRDD(List(1, 2, 3, 4, 5), 3)
    //        val start = ((i * length) / numSlices).toInt
    //        val end = (((i + 1) * length) / numSlices).toInt
    //        (start, end)


    // 将我们的数据处理完保存为分区文件，以分区为单位
    rdd.saveAsTextFile("output")
    sc.stop()
  }
}
