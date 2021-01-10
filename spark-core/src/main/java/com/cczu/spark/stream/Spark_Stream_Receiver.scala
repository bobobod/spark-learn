package com.cczu.spark.stream

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.dstream.{DStream, InputDStream, ReceiverInputDStream}
import org.apache.spark.streaming.receiver.Receiver
import org.apache.spark.streaming.{Seconds, StreamingContext}

import scala.collection.mutable
import scala.util.Random

/**
 *
 * @author jianzhen.yin
 * @date 2020/12/26
 */
object Spark_Stream_Receiver {
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setMaster("local").setAppName("spark stream")
    // 第二个参数是采集周期
    val ssc = new StreamingContext(conf, Seconds(3))
    val ds: ReceiverInputDStream[String] = ssc.receiverStream(new MyReceiver)
    ds.print()

    ssc.start()

    ssc.awaitTermination()

  }

  class MyReceiver extends Receiver[String](StorageLevel.MEMORY_ONLY) {
    private var flag = true

    override def onStart(): Unit = {
      new Thread(
        new Runnable {
          override def run(): Unit = {

            while (flag) {
              val message: String = "采集的数据为" + new Random().nextInt(10).toString
              // 储存
              store(message)
              Thread.sleep(5000)
            }
          }
        }
      ).start()
    }

    override def onStop(): Unit = {
      flag = false
    }
  }

}
