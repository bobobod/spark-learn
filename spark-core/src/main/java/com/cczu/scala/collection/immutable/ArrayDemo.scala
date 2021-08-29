package com.cczu.scala.collection.immutable

object ArrayDemo {
  def main(args: Array[String]): Unit = {
    // 第一种方式
    val arr: Array[Int] = new Array[Int](10)
    arr(0) = 111

    // 第二种
    val arr2: Array[Int] = Array(12,211,111);

    println(arr2(0))
    // 访问元素
    for (elem <- arr2) {
      println(elem)
    }
    arr2.foreach(println)

    // 添加元素,创建一个新的数组，返回新数组,往后面添加
    val arr3: Array[Int] = arr2.:+(111)
    val arr5: Array[Int] = arr2 :+ 111
    // 往前面添加
    val arr4: Array[Int] = arr2.+:(123)
    val arr6: Array[Int] = 123 +: arr2

    // 创建二维数组
    val arr9: Array[Array[Int]] = Array.ofDim[Int](2, 3)
    arr9(1)(1) = 11

    println(arr.length)
  }
}
