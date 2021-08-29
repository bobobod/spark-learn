package com.cczu.scala.collection.mutable

import scala.collection.mutable


object MutableMapDemo {
  def main(args: Array[String]): Unit = {
    val map: mutable.Map[String, Int] = mutable.Map("a" -> 3)
    val map2: mutable.Map[String, Int] = mutable.Map("a" -> 3)
    map.put("abc",2)
    map.remove("abc")
    map.update("abc",5)
    // 合并
    val map3: mutable.Map[String, Int] = map ++ map2
  }

}
