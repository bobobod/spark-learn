package com.cczu.spark.rdd.io

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 *
 * @author jianzhen.yin
 * @date 2020/12/26
 */
object RDD_Load {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[*]").setAppName("rdd memory")
    val sc = new SparkContext(conf)
    val rdd: RDD[String] = sc.textFile("output")
    println(rdd.collect().mkString(","))
    val rdd1: RDD[(String, Int)] = sc.objectFile[(String, Int)]("output1")
    println(rdd1.collect().mkString(","))
    val rdd2: RDD[(String, Int)] = sc.sequenceFile[String, Int]("output2")
    println(rdd2.collect().mkString(","))

    sc.stop()
  }

}
