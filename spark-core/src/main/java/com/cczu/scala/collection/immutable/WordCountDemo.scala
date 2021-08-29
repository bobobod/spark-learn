package com.cczu.scala.collection.immutable

object WordCountDemo {
  def main(args: Array[String]): Unit = {
    val list: List[String] = List(
      "hello",
      "hello spark",
      "hello flink",
      "hello world"
    )
    val flatMap: List[String] = list.flatMap(_.split(" "))
    val groupBy: Map[String, List[String]] = flatMap.groupBy(word => word)
    println(groupBy)
    val countMap: Map[String, Int] = groupBy.map(item => (item._1, item._2.length))
    // 将map转换成list，并排序
    val sortList: List[(String, Int)] = countMap.toList.sortWith(_._2 > _._2).take(3)
    println(sortList)
  }
}
