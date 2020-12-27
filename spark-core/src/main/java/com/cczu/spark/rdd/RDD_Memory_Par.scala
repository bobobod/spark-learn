package com.cczu.spark.rdd

import org.apache.spark.{SparkConf, SparkContext}

/**
 *
 * @author jianzhen.yin
 * @date 2020/12/27
 */
object RDD_Memory_Par {
  def main(args: Array[String]): Unit = {
    // 括号* 表示当前系统最大可用核数 ，不写表示单核
    val conf = new SparkConf().setMaster("local[*]").setAppName("rdd memory")
    val sc = new SparkContext(conf)
    // RDD的并行度和分区 每个分区对应一个task，如果每个task都有对应的cpu核执行，那么并行度核分区数是一致的，如果cpu核数少于task，则并行度为cpu核数
    // 第二个参数是分区数 numSlices
    // 可以不传递这个参数，这样会有一个默认值 defaultParallelism（默认并行数）
    //  scheduler.conf.getInt("spark.default.parallelism", totalCores)
    // 如果获取不到，那么使用totalCores属性，这个属性的值为当前运行环境的最大核数
//    val rdd = sc.makeRDD(
//      List(1, 2, 3, 4), 2
//    )
    val rdd = sc.makeRDD(List(1, 2, 3, 4))
    // 将我们的数据处理完保存为分区文件，以分区为单位
    rdd.saveAsTextFile("output")
    sc.stop()
  }
}
