package com.cczu.spark.rdd.operator.transform

import org.apache.spark.{SparkConf, SparkContext}

/**
 *
 * @author jianzhen.yin
 * @date 2020/12/27
 */
object Oper_Transform_AggregationByKey {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[*]").setAppName("rdd memory")
    val sc = new SparkContext(conf)
    val rdd = sc.parallelize(List(("a",1),("a",2),("a",3),("a",4)),2)
    // [(a,[1,2]),(a,[3,4])]
    // (a,2) (a,4)
    // (a,6)

    // 将数据根据不同的规则进行分区内计算和分区内计算

    // aggregateByKey 有两个参数列表
    // 第一个参数列表  需要传递一个参数，表示初始值，只要用第一个key的时候，value进行分区内计算
    // 第二个参数需要传递2个参数
    //  第一个参数表示分区内计算规则
    //  第二个参数表示分区间计算规则
    rdd.aggregateByKey(0)(
      (x,y) => Math.max(x,y),
      (x,y) => x+y
    ).collect().foreach(println)
    sc.stop()
  }
}
