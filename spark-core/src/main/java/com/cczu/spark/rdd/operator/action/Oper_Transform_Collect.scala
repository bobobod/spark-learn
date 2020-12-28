package com.cczu.spark.rdd.operator.action

import org.apache.spark.{SparkConf, SparkContext}

/**
 *
 * @author jianzhen.yin
 * @date 2020/12/27
 */
object Oper_Transform_Collect {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[*]").setAppName("rdd memory")
    val sc = new SparkContext(conf)
    val rdd = sc.parallelize(List(("a",1),("a",2),("a",3),("a",4)),2)
    val rdd2 = sc.parallelize(List(4,3,2,1),2)
    // 行动算子
    // 所谓的行动算子，其实就是触发job执行的方法
    // 底层代码调用的是 sc.runJob
    // 底层会创建activeJob并执行
    // 不同分区的数据会通过分区顺序采集到driver端的内存
    rdd.collect().foreach(println)
    // rdd 数据的个数
    println(rdd.count())
    // 取第一个
    println(rdd.first())
    // 获取n个数据
    rdd.take(2)
    // 数据排序后取n个数据
    rdd2.takeOrdered(3)
    // 遍历数据，通过什么拼接起来
    rdd2.collect().mkString(",")
    sc.stop()
  }
}
