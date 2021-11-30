package com.cczu.spark.rdd.persist

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 *
 * @author jianzhen.yin
 * @date 2020/12/26
 */
object RDD_Persist {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("word Count")
    val sc = new SparkContext(conf)

    // 读取文件，获取一行一行数据
    val lines: RDD[String] = sc.textFile("data/test1")
    // 将一行进行拆分
    // 扁平化
    val words: RDD[String] = lines.flatMap(_.split(" "))
    val wordToOne: RDD[(String, Int)] = words.map(word => {
      println("--------")
      (word, 1)
    })
    wordToOne.persist()
    //    wordToOne.cache()
    val wordCount: RDD[(String, Int)] = wordToOne.reduceByKey(_ + _)
    val arr: Array[(String, Int)] = wordCount.collect()
    println("!!!!!!!!!!")

    wordToOne.groupByKey().collect()
    for (elem <- arr) {
      println(elem)
    }
    while (true){}

    sc.stop()
  }

}
