package com.cczu.spark.sql

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}

/**
 *
 * @author jianzhen.yin
 * @date 2020/12/26
 */
object Spark_Sql_Basic {
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setMaster("local").setAppName("spark sql")

    val spark: SparkSession = SparkSession.builder().config(conf).getOrCreate()
    import spark.implicits._
    // RDD
    // 1. DataFrame
    //        val df: DataFrame = spark.read.json("data/user")
    //    df.show()
    // DataFrame => sql
    // View 只可以查 不可以该
    //    df.createOrReplaceTempView("user")
    //    spark.sql("select * from user").show()
    //    spark.sql("select avg(age) from user").show()
    // DataFrame => dsl
    // 在使用dataframe时，如果设计到转换操作，需要引入转换规则
    //
    //    df.select($"age" + 1).show()
    //    df.select('age + 1).show()
    // 2. DataSet
    // dataframe是特定泛型的dataset
    //    val seq = Seq(1, 2, 3, 4)
    //    val ds: Dataset[Int] = seq.toDS()
    //    ds.show()

    // rdd -> dataframe
    val rdd: RDD[(Int, String)] = spark.sparkContext.makeRDD(List((1, "zhangsan"), (2, "lisi")))
    val df: DataFrame = rdd.toDF("age", "username")
    val dfToRdd: RDD[Row] = df.rdd

    // dataframe ->  dataset
    val dataSet: Dataset[User] = df.as[User]
    val dsToDf: DataFrame = dataSet.toDF()

    // rdd -> dataset
    val rddToDs: Dataset[User] = rdd.map {
      case (age, username) => {
        User(age, username)
      }
    }.toDS()
    val dsToRdd: RDD[User] = rddToDs.rdd
    spark.close()
  }

  case class User(age: Int, username: String)

}
