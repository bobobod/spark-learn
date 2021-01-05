package com.cczu.spark.rdd.dep

import org.apache.spark.{SparkConf, SparkContext}

/**
 *
 * @author jianzhen.yin
 * @date 2020/12/26
 */
object RDD_dep {
  def main(args: Array[String]): Unit = {
    // TODO 建立连接
    val conf = new SparkConf().setMaster("local").setAppName("word Count")
    val sc = new SparkContext(conf)

    // 读取文件，获取一行一行数据
    val lines = sc.textFile("data")

    println(lines.toDebugString)
    println("-------------")
    // 将一行进行拆分
    // 扁平化
    val words = lines.flatMap(_.split(" "))
    println(words.toDebugString)
    println("-------------")
    // 将单词进行分组，便于统计
    // (hello,hello,hello) (scala,scala)
    val wordToOne = words.map(word => (word, 1))
    println(wordToOne.toDebugString)
    println("-------------")
    val wordCount = wordToOne.reduceByKey(_ + _)
    println(wordCount.toDebugString)
    println("-------------")
    // 将转换结果展示
    val arr = wordCount.collect()
    for (elem <- arr) {
      println(elem)
    }
    sc.stop()
  }

}
