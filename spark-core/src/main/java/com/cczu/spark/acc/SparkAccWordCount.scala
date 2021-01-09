package com.cczu.spark.acc

import org.apache.spark.rdd.RDD
import org.apache.spark.util.{AccumulatorV2, LongAccumulator}
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable

/**
 *
 * @author jianzhen.yin
 * @date 2021/1/9
 */
object SparkAccWordCount {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local").setAppName("word Count")
    val sc = new SparkContext(conf)
    val rdd: RDD[String] = sc.parallelize(List("hello", "spark", "hello"))
    /*
    累加器：wordCount
    1。创建累加器对象
    2。向spark中注册累加器
    3。
     */
    val accumulator = new MyAccumulator()
    sc.register(accumulator, "acc")
    rdd.foreach(item => {
      accumulator.add(item)
    })
    println(accumulator.value)
    sc.stop()
  }

  /*
  IN :累加器需要输入的类型
  OUT：累加器需要输出的类型
   */
  class MyAccumulator extends AccumulatorV2[String, mutable.Map[String, Long]] {
    private var wcMap = mutable.Map[String, Long]()

    override def isZero: Boolean = {
      wcMap.isEmpty
    }

    override def copy(): AccumulatorV2[String, mutable.Map[String, Long]] = {
      new MyAccumulator()
    }

    override def reset(): Unit = {
      wcMap.clear()
    }

    override def add(v: String): Unit = {

      val newVal = wcMap.getOrElse(v, 0L) + 1L
      wcMap.update(v, newVal)
    }

    // Drive中合并多个累加器
    override def merge(other: AccumulatorV2[String, mutable.Map[String, Long]]): Unit = {
      val map1 = this.wcMap
      var map2 = other.value
      map2.foreach(

        item => {
          val newVal = map1.getOrElse(item._1, 0L) + item._2
          map1.update(item._1, newVal)
        }
      )

    }

    // 累加器的结果
    override def value: mutable.Map[String, Long] = {
      wcMap
    }
  }

}
