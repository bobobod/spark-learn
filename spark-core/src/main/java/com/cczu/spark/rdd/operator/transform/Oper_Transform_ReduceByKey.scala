package com.cczu.spark.rdd.operator.transform

import org.apache.spark.{SparkConf, SparkContext}

/**
 *
 * @author jianzhen.yin
 * @date 2020/12/27
 */
object Oper_Transform_ReduceByKey {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[*]").setAppName("rdd memory")
    val sc = new SparkContext(conf)
    // reduceByKey默认也会shuffle
    val rdd = sc.parallelize(List(("a",1),("a",1),("a",2),("b",3)),2)
    // 相同的key 对value进行聚合
    // spark和scala一样是两两聚合的
    // reduceByKey 如果key只有1个是不会参与计算的
    rdd.reduceByKey((x,y)=>{
      println(s"x=$x,y=$y")
      x+y
    }).collect().foreach(println)
     sc.stop()
  }
}
