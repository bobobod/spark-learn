package com.cczu.spark.rdd.operator.transform

import org.apache.spark.{SparkConf, SparkContext}

/**
 *
 * @author jianzhen.yin
 * @date 2020/12/27
 */
object Oper_Transform_GroupByKey {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[*]").setAppName("rdd memory")
    val sc = new SparkContext(conf)
    val rdd = sc.parallelize(List(("a",1),("a",2),("a",3),("b",1)),2)
    // groupByKey:将数据源中数据，相同key分在一个组中，形成一个对偶元祖
    // 元组中第一个元素是key，第二个元素是相同key的value集合
    // reduceByKey和groupByKey的区别
    // 1。两者都存在shuffle操作。但是reduceByKey支持分区预聚合，可以减少shuffle落盘的数据量
    // 2。从功能的角度，reduceByKey支持分组和聚合，而groupByKey只支持分组，不能聚合
    //spark，shuffle操作必须落盘处理，不能在内存中数据等待，会导致内存溢出。shuffle操作会影响性能

    // reduceByKey 分区内和分区间 的计算规则是一样的
    rdd.groupByKey().collect().foreach(println)

    rdd.groupBy(_._1).collect().foreach(println)
    sc.stop()
  }
}
