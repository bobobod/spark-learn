package com.cczu.spark.rdd.operator.transform

import org.apache.spark.{SparkConf, SparkContext}

/**
 *
 * @author jianzhen.yin
 * @date 2020/12/27
 */
object Oper_Transform_CombineByKey {
  /*

   */
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[*]").setAppName("rdd memory")
    val sc = new SparkContext(conf)
    val rdd = sc.parallelize(List(("a", 1), ("a", 2), ("a", 3), ("a", 4)), 2)
    //  combineByKey 需要三个参数
    // 第一个参数：将相同的key进行结构化转化，实现操作
    // 第二个参数：分区内计算规则
    // 第三个参数：分区间计算规则
    val newRDD = rdd.combineByKey(v => (v, 1), (t: (Int, Int), v) => (t._1 + v, t._2 + 1),
      (t1: (Int, Int), t2: (Int, Int)) => {
        (t1._1 + t2._1, t2._2 + t2._2)
      })
    newRDD.mapValues {
      case (num, cnt) =>
        num / cnt

    }
    sc.stop()
  }
}
