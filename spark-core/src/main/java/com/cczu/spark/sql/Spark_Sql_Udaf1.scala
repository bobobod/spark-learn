package com.cczu.spark.sql

import org.apache.spark.SparkConf
import org.apache.spark.sql.expressions.Aggregator
import org.apache.spark.sql.{DataFrame, Encoder, Encoders, Row, SparkSession, functions}

/**
 *
 * @author jianzhen.yin
 * @date 2020/12/26
 */
object Spark_Sql_Udaf1 {
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setMaster("local").setAppName("spark sql")

    val spark: SparkSession = SparkSession.builder().config(conf).getOrCreate()
    // RDD
    // 1. DataFrame
    val df: DataFrame = spark.read.json("data/user")
    //    df.show()
    // DataFrame => sql
    // View 只可以查 不可以该
    df.createOrReplaceTempView("user")
    spark.udf.register("ageAvg", functions.udaf(new MyAvgUDAF()))

    spark.sql("select ageAvg(age) from user").show()

    spark.close()
  }

  /*
  自定义强类型聚合函数
   */
  // 加入var是为了让属性值可变
  case class Buff(var total: Long, var count: Long)

  class MyAvgUDAF extends Aggregator[Long, Buff, Long] {
    // 初始值 z&zero
    override def zero: Buff = {
      Buff(0L, 0L)
    }

    //根据输入数据更新缓冲区数据
    override def reduce(buff: Buff, in: Long): Buff = {
      buff.total = buff.total + in
      buff.count = buff.count + 1
      buff
    }

    // 合并缓冲区
    override def merge(b1: Buff, b2: Buff): Buff = {
      b1.count = b1.count + b2.count
      b1.total = b1.total + b2.total
      b1
    }

    // 计算结果
    override def finish(reduction: Buff): Long = {
      reduction.total / reduction.count
    }

    // 缓冲区的编码操作
    override def bufferEncoder: Encoder[Buff] = Encoders.product

    // 输出的编码操作
    override def outputEncoder: Encoder[Long] = Encoders.scalaLong
  }

}
