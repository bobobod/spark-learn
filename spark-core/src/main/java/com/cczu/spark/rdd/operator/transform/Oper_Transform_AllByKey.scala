package com.cczu.spark.rdd.operator.transform

import org.apache.spark.{SparkConf, SparkContext}

/**
 *
 * @author jianzhen.yin
 * @date 2020/12/27
 */
object Oper_Transform_AllByKey {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[*]").setAppName("rdd memory")
    val sc = new SparkContext(conf)
    val rdd = sc.parallelize(List(("a", 1), ("a", 2), ("a", 3), ("a", 4)), 2)
    /*
      reduceByKey
            combineByKeyWithClassTag[V]((v: V) => v, func, func, partitioner)
      aggregateByKey
            combineByKeyWithClassTag[U]((v: V) => cleanedSeqOp(createZero(), v),
      foldByKey
            combineByKeyWithClassTag[V]((v: V) => cleanedFunc(createZero(), v),
      combineByKey
            combineByKeyWithClassTag(
            createCombiner, 初始值和第一个key的value值数据操作
            mergeValue,    分区内计算规则
            mergeCombiners, 分区间计算规则
             defaultPartitioner(self))
     */
    rdd.reduceByKey(_ + _) // wordCount
    rdd.aggregateByKey(0)(_ + _, _ + _)
    rdd.foldByKey(0)(_ + _)
    rdd.combineByKey(v => v, (x: Int, y) => x + y, (x: Int, y: Int) => x + y)
    sc.stop()
  }
}
