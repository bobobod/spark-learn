package com.cczu.spark.rdd.dep

import org.apache.spark.{SparkConf, SparkContext}

/**
 *
 * @author jianzhen.yin
 * @date 2020/12/26
 */
object RDD_dep2 {
  /**
   * OneToOneDependency 新的RDD的一个分区的数据依赖依赖旧的RDD的一个分区
   * ShuffleDependency 新的RDD的一个分区的数据依赖依赖旧的RDD的多个分区
   * @param args
   */
  def main(args: Array[String]): Unit = {
    // TODO 建立连接
    val conf = new SparkConf().setMaster("local").setAppName("word Count")
    val sc = new SparkContext(conf)

    // 读取文件，获取一行一行数据
    val lines = sc.textFile("data")

    println(lines.dependencies)
    println("-------------")
    // 将一行进行拆分
    // 扁平化
    val words = lines.flatMap(_.split(" "))
    println(words.dependencies)
    println("-------------")
    // 将单词进行分组，便于统计
    // (hello,hello,hello) (scala,scala)
    val wordToOne = words.map(word => (word, 1))
    println(wordToOne.dependencies)
    println("-------------")
    val wordCount = wordToOne.reduceByKey(_ + _)
    println(wordCount.dependencies)
    println("-------------")
    // 将转换结果展示
    val arr = wordCount.collect()
    for (elem <- arr) {
      println(elem)
    }
    sc.stop()
  }

}
