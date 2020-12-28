package com.cczu.spark.rdd.operator.transform

import org.apache.spark.{SparkConf, SparkContext}

/**
 *
 * @author jianzhen.yin
 * @date 2020/12/27
 */
object Oper_Transform_Join {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[*]").setAppName("rdd memory")
    val sc = new SparkContext(conf)
    val rdd = sc.parallelize(List(("a",1),("b",2),("c",3),("d",4)))
    val rdd2 = sc.parallelize(List(("a",4),("b",5),("c",6),("e",7)))
    // 相同key会形成元组
    // 如果两个数据源，可能会出现笛卡尔积，数据量会几何形增长
    rdd.join(rdd2).collect().foreach(println)
    println("-------")
    rdd.leftOuterJoin(rdd2).collect().foreach(println)
    println("-------")
    rdd.rightOuterJoin(rdd2).collect().foreach(println)
    sc.stop()
  }
}
