package com.cczu.scala.collection.immutable

object MapDemo {
  def main(args: Array[String]): Unit = {
    val map: Map[String, Int] = Map("a" -> 2,"b" ->3)
    for (elem <- map) {
      println(elem._1,elem._2)
    }
    for (key <- map.keySet){
      println(key)
    }
    val value: Option[Int] = map.get("333")
    println(value)
    println(map.get("a").get)
    println(map.getOrElse("3",0))
    println(map("a"))
  }

}
