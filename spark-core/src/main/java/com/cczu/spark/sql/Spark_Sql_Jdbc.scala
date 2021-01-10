package com.cczu.spark.sql

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset, Row, SaveMode, SparkSession}

/**
 *
 * @author jianzhen.yin
 * @date 2020/12/26
 */
object Spark_Sql_Jdbc {
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setMaster("local").setAppName("spark sql")

    val spark: SparkSession = SparkSession.builder().config(conf).getOrCreate()
    import spark.implicits._
    // 读取
    val df: DataFrame = spark.read
      .format("jdbc")
      .option("url", "jdbc:mysql://localhost:3306/gulimall_pms")
      .option("driver", "com.mysql.cj.jdbc.Driver")
      .option("user", "root")
      .option("password", "root")
      .option("dbtable", "pms_attr")
      .load()
    df.show()
    // 写入
    df.write
        .format("jdbc")
        .option("url","jdbc:mysql://localhost:3306/gulimall_pms")
        .option("driver","com.mysql.cj.jdbc.Driver")
        .option("user","root")
        .option("password","root")
        .option("dbtable","pms_attr2")
        .mode(SaveMode.Append)
        .save()
    spark.close()
  }

  case class User(age: Int, username: String)

}
