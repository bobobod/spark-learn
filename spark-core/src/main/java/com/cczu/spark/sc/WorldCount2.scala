package com.cczu.spark.sc

import org.apache.spark.{SparkConf, SparkContext}

/**
 *
 * @author jianzhen.yin
 * @date 2020/12/26
 */
object WorldCount2 {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local").setAppName("word Count")
    val sc = new SparkContext(conf)

    // 读取文件，获取一行一行数据
    val lines = sc.textFile("data")
    // 将一行进行拆分
    // 扁平化
    val words = lines.flatMap(_.split(" "))
    val wordToOne = words.map(word => (word, 1))
    // 将转换结果展示
    val wordGroup = wordToOne.groupBy(t => t._1)
    val wordCount = wordGroup.map {
      case (word, list) => {
        list.reduce((t1, t2) => (t1._1, t2._2 + t1._2))
      }
    }
    val arr = wordCount.collect()
    for (elem <- arr) {
      println(elem)
    }
    sc.stop()
  }

}
