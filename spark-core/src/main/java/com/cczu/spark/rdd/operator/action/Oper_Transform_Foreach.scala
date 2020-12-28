package com.cczu.spark.rdd.operator.action

import org.apache.spark.{SparkConf, SparkContext}

/**
 *
 * @author jianzhen.yin
 * @date 2020/12/27
 */
object Oper_Transform_Foreach {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[*]").setAppName("rdd memory")
    val sc = new SparkContext(conf)
    val rdd2 = sc.parallelize(List(4,3,2,1),2)
    // 从driver端的内存集合循环遍历
    rdd2.collect().foreach(println)
    // 从executor中循环遍历元素，分布式节点中执行
    // 特别注意：RDD 的方法外部的操作都是在driver端执行的，而方法内部的逻辑代码在Executor中执行的
    // collect() 在driver中执行的，println在
    rdd2.foreach(println)

    // user是外部操作是在driver中执行的
    val user = User()

    // RDD算子中传递的函数会包含闭包操作，会进行检测功能
    rdd2.foreach(line =>{
      println(user.age + line)
    })
    sc.stop()
  }
  // 样例类在编译时，自动实现了序列化
  case class User(){
    var age:Int = 20
  }
}
