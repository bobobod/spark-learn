package com.cczu.spark.rdd.operator.transform

import org.apache.spark.{SparkConf, SparkContext}

/**
 *
 * @author jianzhen.yin
 * @date 2020/12/27
 */
object Oper_Transform_FoldByKey {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[*]").setAppName("rdd memory")
    val sc = new SparkContext(conf)
    val rdd = sc.parallelize(List(
      ("a",1),("a",2),("a",3),("a",4)
    ),2)
    // [(a,[1,2]),(a,[3,4])]
    // (a,2) (a,4)
    // (a,6)

//    rdd.aggregateByKey(0)(_+_,_+_).collect().foreach(println)

    // 如果两个聚合计算相同，分区内和分区间的规则相同时，可以使用foldByKey
    rdd.foldByKey(0)(_+_).collect().foreach(println)
    sc.stop()
  }
}
