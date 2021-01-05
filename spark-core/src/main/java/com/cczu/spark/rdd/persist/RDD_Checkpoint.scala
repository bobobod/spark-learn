package com.cczu.spark.rdd.persist

import org.apache.spark.{SparkConf, SparkContext}

/**
 *
 * @author jianzhen.yin
 * @date 2020/12/26
 */
object RDD_Checkpoint {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local").setAppName("word Count")
    val sc = new SparkContext(conf)
    // checkpoint 需要落盘，需要指定检查点保存路径
    // 检查点保持的路径一般是Hdfs，当作业执行完毕后，不会删除数据
    sc.setCheckpointDir("checkpoint")

    val words = sc.parallelize(List("a", "b"))
    val wordToOne = words.map(word => {
      println("--------")
      (word, 1)
    })
//    wordToOne.cache()
    wordToOne.checkpoint()
    //    wordToOne.cache()
    val wordCount = wordToOne.reduceByKey(_ + _)
    val arr = wordCount.collect()
    println("!!!!!!!!!!")

    wordToOne.groupByKey().collect()
    for (elem <- arr) {
      println(elem)
    }
    sc.stop()
  }

}
