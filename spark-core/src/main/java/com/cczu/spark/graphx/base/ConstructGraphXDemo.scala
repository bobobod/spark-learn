package com.cczu.spark.graphx.base

import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * 结构操作
 * reverse：边的方向反转
 * subgraph:过滤子图，订单和边都会过滤
 * mask：构造一个子图，这个操作可以和subgraph操作相结合，基于另外一个相关图的特征去约束一个图。
 * groupEdges:合并多重图中的并行边（如定点对之间的重复边）
 */
object ConstructGraphXDemo {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local").setAppName("graphx")
    val sc: SparkContext = new SparkContext(conf)
    // Create an RDD for the vertices
    val users: RDD[(VertexId, (String, String))] =
      sc.parallelize(
        Array((3L, ("rxin", "student")), (7L, ("jgonzal", "postdoc")),
          (5L, ("franklin", "prof")), (2L, ("istoica", "prof")),
          (4L, ("peter", "student"))))
    // Create an RDD for edges
    val relationships: RDD[Edge[String]] =
      sc.parallelize(
        Array(Edge(3L, 7L, "collab"), Edge(5L, 3L, "advisor"),
          Edge(2L, 5L, "colleague"), Edge(5L, 7L, "pi"),
          Edge(4L, 0L, "student"), Edge(5L, 0L, "colleague")
          , Edge(3L, 7L, "testGroupEdges")))
    // Define a default user in case there are relationship with missing user
    val defaultUser: (String, String) = ("John Doe", "Missing")
    // Build the initial Graph
    //  如果边对应的点不存在的话，会用到这个默认用户
    val graph: Graph[(String, String), String] = Graph(users, relationships, defaultUser)
    // 如果没有设置默认用户的话，不存在的点的属性为null
    //    val graph: Graph[(String, String), String] = Graph(users, relationships)
    graph.triplets.foreach(item => println(s"${item.srcId}---${item.srcAttr}---${item.dstId}---${item.dstAttr}---${item.attr}"))

    println("-------reverse--------")
    val reverseGraph: Graph[(String, String), String] = graph.reverse
    reverseGraph.triplets.foreach(item => println(s"${item.srcId}---${item.srcAttr}---${item.dstId}---${item.dstAttr}---${item.attr}"))

    println("-----subgraph----------")
    // 有效的子图，过滤属性为空的顶点,同时把边也会进行过滤
    val validSubGraph: Graph[(String, String), String] = graph.subgraph(vpred = (id, attr) => !"Missing".equals(attr._2))
    validSubGraph.triplets
      .map(item => s"${item.srcId}---${item.srcAttr}---${item.dstId}---${item.dstAttr}---${item.attr}").collect().foreach(println)
    println("-------mask--------")
    // 跑联通图后
    val connectedGraph: Graph[VertexId, String] = graph.connectedComponents()
    // 顶点格式（vid，群组里最小的vid作为群组id） 边格式不变
    //    println(connectedGraph.vertices.collect().toList)
    //    println(connectedGraph.edges.collect().toList)
    //    println(connectedGraph.triplets.collect().toList)
    // 去掉不存在的顶点
    val validGraph: Graph[(String, String), String] = graph.subgraph(vpred = (id, attr) => attr._2 != "Missing")
    // Restricts the graph to only the vertices and edges that are also in `other`
    // , but keeps the attributes from this graph.
    // 翻译：通过other图来过滤当前图，只保留在other图里面也存在的实体和边，但是保留属性为当前图的属性
    val maskGraph: Graph[VertexId, String] = connectedGraph.mask(validGraph)
    println(maskGraph.vertices.collect().toList)
    println(maskGraph.edges.collect().toList)
    println(maskGraph.triplets.collect().toList)
    println("-------groupEdges--------")
    // 合并src顶点和dst顶点一样同时边的方向一样的多条边成单条边
    val groupEdgesGraph: Graph[(String, String), String] = graph.groupEdges {
      (str1, str2) => {
        str1
      }
    }
    println(groupEdgesGraph.vertices.collect().toList)
    println(groupEdgesGraph.edges.collect().toList)
    println(groupEdgesGraph.triplets.collect().toList)
    sc.stop()

  }
}
