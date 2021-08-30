package com.cczu.scala.base

object TypeConvertDemo {
  def main(args: Array[String]): Unit = {
    // 1.类型自动提升
    val a1: Byte = 1
    val b2: Long = 1L
    val l: Long = a1 + b2
    val l2: Int = (a1 + b2).toInt
    // 2.精度大的赋值给精度小的，会报错，反之可以
    val c2: Int = a1
    // 3.(byte,short)和char之间不能自动互换
    val b3: Char = 'A'
    //    val a2:Byte = b3  error
    val a3: Int = b3
    // 4. byte,short,char 三者可以相互运算，会被转换成int

    // yield会把当前元素记下来，放入集合

    // _* 用法
    def sum(ints: Int*): Int = {
      var res: Int = 0
      for (elem <- ints) {
        res += elem
      }
      res
    }

    println(sum(1, 2, 3, 4, 5))
    // 将 1 to 2 转成参数序列
    println(sum(1 to 2: _*))
  }
}
