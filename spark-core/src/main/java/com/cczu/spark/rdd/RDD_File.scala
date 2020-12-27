package com.cczu.spark.rdd

import org.apache.spark.{SparkConf, SparkContext}

/**
 *
 * @author jianzhen.yin
 * @date 2020/12/27
 */
object RDD_File {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[*]").setAppName("rdd memory")
    val sc = new SparkContext(conf)
    // textFile 是以行为单位读取文件的
    // wholeTextFiles 是以文件为单位的 读取的结果是一个元组，第一个元素是文件路径，第二个元素是内容
    val rdd = sc.wholeTextFiles("data")
    rdd.collect().foreach(println)
    sc.stop()
  }

}
