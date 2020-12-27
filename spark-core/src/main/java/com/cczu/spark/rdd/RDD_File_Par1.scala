package com.cczu.spark.rdd

import org.apache.spark.{SparkConf, SparkContext}

/**
 *
 * @author jianzhen.yin
 * @date 2020/12/27
 */
object RDD_File_Par1 {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[*]").setAppName("rdd memory")
    val sc = new SparkContext(conf)
    // 数据分区的分配
    // 1.数据是以行为单位读取的
    //  spark读取文件，采用的是hadoop的读取方式是和hadoop一样，是一行一行读取的，和字节数无关
    // 偏移量不会被重新读取
    // 2。数据读取时以偏移量为单位的，偏移量不能被重复读取
   /*
   1@@  012
   2@@  345
   3    6
    */
    // 3.数据分区偏移量范围的计算
    // 0 =》 【0，3】 =》 【1，2】
    // 1 =》 【3，6】=》 【3】
    // 2 =》 【6，7】

    // 【1，2】，【3】，【】
    val rdd = sc.textFile("data/test3",2)
    rdd.saveAsTextFile("output")
    sc.stop()
  }

}
