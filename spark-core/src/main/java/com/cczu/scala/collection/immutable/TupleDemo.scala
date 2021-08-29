package com.cczu.scala.collection.immutable

object TupleDemo {
  def main(args: Array[String]): Unit = {
    val tuple: (Int, Int, String, String) = (1, 3, "afdas", "1")
    for (elem <- tuple.productIterator){
      println(elem)
    }
  }

}
