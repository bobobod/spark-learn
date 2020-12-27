package com.cczu.spark.rdd

import org.apache.spark.{SparkConf, SparkContext}

/**
 *
 * @author jianzhen.yin
 * @date 2020/12/27
 */
object RDD_File_Par {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[*]").setAppName("rdd memory")
    val sc = new SparkContext(conf)
    //minPartitions：最小分区数 默认值   def defaultMinPartitions: Int = math.min(defaultParallelism, 2)
    // spark读取文件的方式就是hadoop读取文件的方式
    // 分区数量的计算方式
    // test3.txt 总共有7个字节
    // totalSize = 7
    // goalSize = totalSize / minPartitions = 7/2 = 3
    // 7 / 3 = 2 ... 1 (其中1/3 大于 10% ，则新建一个分区，所有有3个）
    val rdd = sc.textFile("data/test3",2)
    rdd.saveAsTextFile("output")
    sc.stop()
  }

}
