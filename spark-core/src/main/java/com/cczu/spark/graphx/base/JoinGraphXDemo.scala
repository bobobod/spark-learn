package com.cczu.spark.graphx.base

import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * 关联操作
 * joinVertices
 * outJoinVertices
 */
object JoinGraphXDemo {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local").setAppName("graphx")
    val sc: SparkContext = new SparkContext(conf)
    // Create an RDD for the vertices
    val users: RDD[(VertexId, (String, String))] =
      sc.parallelize(
        Array((3L, ("rxin", "student")), (7L, ("jgonzal", "postdoc")),
          (5L, ("franklin", "prof")), (2L, ("istoica", "prof")),
          (4L, ("peter", "student"))))
    val users2: RDD[(VertexId, (String, String))] =
      sc.parallelize(
        Array((0L, ("test", "student")),(3L,("test2","student"))))
    // Create an RDD for edges
    val relationships: RDD[Edge[String]] =
      sc.parallelize(
        Array(Edge(3L, 7L, "collab"), Edge(5L, 3L, "advisor"),
          Edge(2L, 5L, "colleague"), Edge(5L, 7L, "pi"),
          Edge(4L, 0L, "student"), Edge(5L, 0L, "colleague")))
    // Define a default user in case there are relationship with missing user
    val defaultUser: (String, String) = ("John Doe", "Missing")
    // Build the initial Graph
    //  如果边对应的点不存在的话，会用到这个默认用户
    val graph: Graph[(String, String), String] = Graph(users, relationships, defaultUser)

    println("------joinVertices----")
    // 根据关联外部rdd，进行vertex属性的更新
    // 如果当前图的实体和外部rdd 没有可以关联上的实体，就用老的属性
    println(graph.joinVertices(users2) {
      case (_, oldAttr, newAttr) => {
        println(s"${oldAttr._1}---${oldAttr._2}")
        (newAttr._1, newAttr._2)
      }
    }.triplets.collect().toList)

    println("------outerJoinVertices----")
    // outerJoin ,外关联，如果图的实体没有关联上也会被遍历，这一点和joinVertices不同
    println(graph.outerJoinVertices(users2) {
      case (_, oldAttr, newAttr) => {
        println(s"${oldAttr._1}---${oldAttr._2}")
        newAttr match {
          case Some(value) => value
          case None => oldAttr
        }
      }
    }.triplets.collect().toList)
    sc.stop()

  }
}
