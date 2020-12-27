package com.cczu.spark.rdd.operator.transform

import org.apache.spark.{SparkConf, SparkContext}

/**
 *
 * @author jianzhen.yin
 * @date 2020/12/27
 */
object Oper_Transform_MapPartitions_Par2 {
  /**
   * map和mapPartitions的区别
   * 1。数据处理的角度
   *    map算子是分区内一个数据一个数据的执行，类似串行操作，而mapPartitions算子是以分区为单位的批处理模式
   * 2。功能的角度
   *    map算子主要的目的是将数据源中的数据进行转换和改变，但是不会减少或增加数据。
   *    mapPartitions算子需要传递一个迭代器，返回一个迭代器，没有要求元素的个数不变，可以增加或减少数据。
   * 3。性能的角度
   *    map算子类似串行操作，所以性能较低，而mapPartitions算子性能较高。但是mapPartitions算子会长时间占用内存，可能会导致内存溢出的情况
   * @param args
   */
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[*]").setAppName("rdd memory")
    val sc = new SparkContext(conf)
    val rdd = sc.parallelize(List(1, 2, 3, 4),2)
    // mapPartitions 是一次性把一个分区内的数据全部拿过来，而map是一个一个处理
    //  可以以分区为单位进行数据转换操作，但是会将整个分区的分区的数据加载到内存中，如果处理完的数据是不会被释放的，存在对象引用
    // 如果内存较小，数据量较大，容易出现内存溢出
    val mapPartitions = rdd.mapPartitions(iter => {
      println(">>>>>")
      iter.map(_ * 2)
    })
    mapPartitions.collect().foreach(println)
    sc.stop()
  }
}
