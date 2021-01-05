package com.cczu.spark.rdd.serial

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 *
 * @author jianzhen.yin
 * @date 2020/12/27
 */
object RDD_Serial {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[*]").setAppName("rdd memory")
    val sc = new SparkContext(conf)
    val rdd = sc.parallelize(Array("hello world", "hello spark", "atguigu", "hive"))
    var search = new Search("h")
    search.getMatch1(rdd).collect().foreach(println)

    search.getMatch2(rdd).collect().foreach(println)
    sc.stop()
  }
 //  类的构造参数其实是类的属性，构造参数需要闭包检测
  class Search(query: String) extends Serializable {
    def isMatch(s: String): Boolean = {
      s.contains(query)
    }

    // 函数序列化案例
    def getMatch1(rdd: RDD[String]): RDD[String] = {
      rdd.filter(isMatch)
    }

    // 属性序列化案例
    def getMatch2(rdd: RDD[String]): RDD[String] = {
      // 解决方案2
      var s = query
//      rdd.filter(x => x.contains(query))
      rdd.filter(x => x.contains(s))
    }
  }

}
