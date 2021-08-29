package com.cczu.scala.base

import java.io.{File, PrintWriter}

import scala.io.{Source, StdIn}

object IODemo {
  def main(args: Array[String]): Unit = {
    //控制台输入
//    val str: String = StdIn.readLine()
//    println(str)

    // 文件输入
    Source.fromFile("data/test1").foreach(print)
    // 文件写入
    val writer: PrintWriter = new PrintWriter(new File("data/test11"))
    writer.write("write file from java")
    writer.close()
  }
}
