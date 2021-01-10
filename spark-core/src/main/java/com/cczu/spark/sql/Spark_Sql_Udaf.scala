package com.cczu.spark.sql

import org.apache.spark.SparkConf
import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types.{DataType, LongType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

/**
 *
 * @author jianzhen.yin
 * @date 2020/12/26
 */
object Spark_Sql_Udaf {
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
    spark.udf.register("ageAvg", new MyAvgUDAF())

    spark.sql("select ageAvg(age) from user").show()

    spark.close()
  }

  /*
  自定义聚合函数
   */
  class MyAvgUDAF extends UserDefinedAggregateFunction {
    // 输入数据的结构
    override def inputSchema: StructType = {
      StructType(Array(StructField("age", LongType)))
    }

    // 缓冲区的结构
    override def bufferSchema: StructType = {
      StructType(Array(StructField("total", LongType), StructField("count", LongType)))

    }

    // 计算结果的数据类型
    override def dataType: DataType = LongType

    // 函数的稳定性
    override def deterministic: Boolean = true

    override def initialize(buffer: MutableAggregationBuffer): Unit = {
      buffer.update(0, 0L)
      buffer.update(1, 0L)
    }

    // 根据输入的值更新缓冲区的值
    override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
      buffer.update(1, buffer.getLong(1) + 1)
      buffer.update(0, buffer.getLong(0) + input.getLong(0))
    }

    // 缓冲区数据合并
    override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
      buffer1.update(0, buffer1.getLong(0) + buffer2.getLong(0))
      buffer1.update(1, buffer1.getLong(1) + buffer2.getLong(1))
    }

    // 计算平均值
    override def evaluate(buffer: Row): Any = {
      buffer.getLong(0) / buffer.getLong(1)
    }
  }

}
