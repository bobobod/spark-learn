package com.cczu.spark.sc

import org.apache.spark.{SparkConf, SparkContext}

/**
 *
 * @author jianzhen.yin
 * @date 2020/12/26
 */
object WorldCount3 {
  def main(args: Array[String]): Unit = {
    // TODO 建立连接
    val conf = new SparkConf().setMaster("local").setAppName("word Count")
    val sc = new SparkContext(conf)

    // 读取文件，获取一行一行数据
    val lines = sc.textFile("data")
    // 将一行进行拆分
    // 扁平化
    val words = lines.flatMap(_.split(" "))
    // 将单词进行分组，便于统计
    // (hello,hello,hello) (scala,scala)
    val wordToOne = words.map(word => (word, 1))
    //    val wordCount = wordToOne.reduceByKey((x, y) => {
    //      x + y
    //    })
    val wordCount = wordToOne.reduceByKey(_ + _)
    // 将转换结果展示
    val arr = wordCount.collect()
    for (elem <- arr) {
      println(elem)
    }
    sc.stop()
  }

}
