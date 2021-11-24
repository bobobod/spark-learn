package com.cczu.spark.sql

import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

/**
 *
 * @author jianzhen.yin
 * @date 2020/12/26
 */
object Spark_Sql_Hive {
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setMaster("local").setAppName("spark sql")

    val spark: SparkSession = SparkSession.builder().enableHiveSupport().config(conf).getOrCreate()
    // 使用spark连接外置hive
    // 1. 拷贝hive-site.xml文件到classpath下
    // 2. 启用hive的支持
    // 3. 增加依赖（mysql的驱动）
    spark.sql("""""").cache()
    spark.close()
  }

  case class User(age: Int, username: String)

}
