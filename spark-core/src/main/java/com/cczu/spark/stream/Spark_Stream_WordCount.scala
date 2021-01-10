package com.cczu.spark.stream

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext, StreamingContextState}

import scala.collection.mutable

/**
 *
 * @author jianzhen.yin
 * @date 2020/12/26
 */
object Spark_Stream_WordCount {
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setMaster("local").setAppName("spark stream")
    // 第二个参数是采集周期
    val ssc = new StreamingContext(conf, Seconds(3))
    val queue = new mutable.Queue[RDD[Int]]()
    val in: InputDStream[Int] = ssc.queueStream(queue, oneAtATime = false)
    val mapRDD: DStream[(Int, Int)] = in.map((_, 1))
    val ds: DStream[(Int, Int)] = mapRDD.reduceByKey(_ + _)
    ds.print()
    ssc.start()
    for (i <- 0 to 5){
      queue += ssc.sparkContext.makeRDD(0 to 300,10)
      Thread.sleep(2000)
    }
    // 如果想要优雅的关闭采集器，那么需要新建线程
    // 需要第三方服务增加关闭状态
    new Thread(new Runnable {
      override def run(): Unit = {
        Thread.sleep(5000)

        // 优雅的关闭，会等计算结束后关闭
        while (true)
          {

            // 状态
            if (true){
              if(ssc.getState() == StreamingContextState.ACTIVE){
                ssc.stop(true,true)
              }
              System.exit(0)
            }

          }
      }
    }).start()
    ssc.awaitTermination() // 阻塞
  }

}
