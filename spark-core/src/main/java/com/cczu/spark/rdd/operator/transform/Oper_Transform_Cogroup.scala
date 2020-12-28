package com.cczu.spark.rdd.operator.transform

import org.apache.spark.{SparkConf, SparkContext}

/**
 *
 * @author jianzhen.yin
 * @date 2020/12/27
 */
object Oper_Transform_Cogroup {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[*]").setAppName("rdd memory")
    val sc = new SparkContext(conf)
    val rdd = sc.parallelize(List(("a",1),("b",2)))
    val rdd2 = sc.parallelize(List(("a",4),("b",5),("c",6),("c",7)))
    // cogroup = connect + group 分组连接
    rdd.cogroup(rdd2).collect().foreach(println)
    sc.stop()
  }
}
