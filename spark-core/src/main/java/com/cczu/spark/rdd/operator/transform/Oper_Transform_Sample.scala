package com.cczu.spark.rdd.operator.transform

import org.apache.spark.{SparkConf, SparkContext}

/**
 *
 * @author jianzhen.yin
 * @date 2020/12/27
 */
object Oper_Transform_Sample {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[*]").setAppName("rdd memory")
    val sc = new SparkContext(conf)

    val rdd = sc.parallelize(List(1,2,3,4,5,6,7,8,9,10))
    // 参数1：抽取数据后是否将数据返回 true为放回
    // 参数2：每条数据被抽取的概率
    //    如果抽取不放回，数据源中每条数据的概率
    //    如果抽取放回，表示数据源中每条数据抽取的可能次数
    // 参数3：抽取算法时随机算法的种子
    // 如果不传递第三个参数，那么使用当前系统时间
    // 情况1
//    println(rdd.sample(
//      false,
//      0.4
//      // 1
//    ).collect().mkString(","))
    // 情况2
    println(rdd.sample(
      true,
      2
      // 1
    ).collect().mkString(","))
    sc.stop()
  }
}
