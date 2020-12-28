package com.cczu.spark.sc

import org.apache.spark.{SparkConf, SparkContext}

/**
 *
 * @author jianzhen.yin
 * @date 2020/12/26
 */
object WorldCount4 {
  def main(args: Array[String]): Unit = {
    // TODO 建立连接
    val conf = new SparkConf().setMaster("local").setAppName("word Count")
    val sc = new SparkContext(conf)

    sc.stop()
  }
  def wordCount1(sc:SparkContext):Unit={
    
  }


}
