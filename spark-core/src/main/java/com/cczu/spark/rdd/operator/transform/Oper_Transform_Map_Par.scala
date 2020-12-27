package com.cczu.spark.rdd.operator.transform

import org.apache.spark.{SparkConf, SparkContext}

/**
 *
 * @author jianzhen.yin
 * @date 2020/12/27
 */
object Oper_Transform_Map_Par {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[*]").setAppName("rdd memory")
    val sc = new SparkContext(conf)
    // map 算子
    val rdd = sc.parallelize(List(1, 2, 3, 4))

    // 转换函数
    //    def mapFunction(num: Int): Int = {
    //      num * 2
    //    }

    //    val mapRDD = rdd.map(mapFunction)
    //    val mapRDD = rdd.map((num: Int) => {
    //      num * 2
    //    })
    // 当大括号只有一行时，大括号可以省略 ，如果类型可以推算出来，类型也可以省略，如果参数只有一个时，小括号也可以省略，如果参数在处理
    // 逻辑中只出现一次，且按照顺序出现的，则参数也可以省略

    val mapRDD = rdd.map(_ * 2)
    mapRDD.collect().foreach(println)
    sc.stop()
  }
}
