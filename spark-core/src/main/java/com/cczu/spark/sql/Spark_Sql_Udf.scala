package com.cczu.spark.sql

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}

/**
 *
 * @author jianzhen.yin
 * @date 2020/12/26
 */
object Spark_Sql_Udf {
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setMaster("local").setAppName("spark sql")

    val spark: SparkSession = SparkSession.builder().config(conf).getOrCreate()
    import spark.implicits._
    // RDD
    // 1. DataFrame
    val df: DataFrame = spark.read.json("data/user")
    //    df.show()
    // DataFrame => sql
    // View 只可以查 不可以该
    df.createOrReplaceTempView("user")
    spark.udf.register("prefixName", (name: String) => {
      "name:" + name
    })

    spark.sql("select age,prefixName(username) from user").show()

    spark.close()
  }


}
