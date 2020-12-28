package com.cczu.spark.rdd.operator.action

import org.apache.spark.{SparkConf, SparkContext}

/**
 *
 * @author jianzhen.yin
 * @date 2020/12/27
 */
object Oper_Transform_Save {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[*]").setAppName("rdd memory")
    val sc = new SparkContext(conf)
    val rdd = sc.parallelize(List(("a",1),("a",2),("a",3),("a",4)),2)
    rdd.saveAsTextFile("output")
    rdd.saveAsObjectFile("output1")
    // 要求数据必须是k-v
    rdd.saveAsSequenceFile("output2")
    sc.stop()
  }
}
