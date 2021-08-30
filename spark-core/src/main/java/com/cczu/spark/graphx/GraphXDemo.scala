package com.cczu.spark.graphx

import org.apache.spark.graphx.{Edge, EdgeDirection, EdgeTriplet, Graph, VertexId, VertexRDD}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object GraphXDemo {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local").setAppName("graphx")
    val sc: SparkContext = new SparkContext(conf)
    val vertices: Array[(Long, (String, String))] = Array((3L, ("rxin", "student")), (7L, ("jgonzal", "postdoc")), (5L, ("franklin", "prof")), (2L, ("istoica", "prof")))
    val users: RDD[(Long, (String, String))] = sc.parallelize(vertices)

    val relationships: RDD[Edge[String]] =
      sc.parallelize(Array(Edge(3L, 7L, "collab"), Edge(5L, 3L, "advisor"), Edge(2L, 5L, "colleague"), Edge(5L, 7L, "pi")))
    // Define a default user in case there are relationship with missing user
    val defaultUser: (String, String) = ("John Doe", "Missing")
    // Build the initial Graph
    // 方式一
    val graph: Graph[(String, String), String] = Graph(users, relationships, defaultUser)

    //
    val cnt: Long = graph.vertices.filter { case (id, (name, pos)) => pos == "postdoc" }.count()
    println(cnt)
    val edgeCnt: Long = graph.edges.filter(e => e.srcId > e.dstId).count()
    println(edgeCnt)

    println(graph.vertices.collect().mkString(","))
    val map: String = graph.vertices.map(item => {
      item._2
    }).collect().mkString(",")
    println(map)

    // 三重视图
    // edgeTriplet类继承了Edge，并添加了类属性 srcAttr、dstAttr
    graph.triplets.map(item => item.srcAttr._1 + " is the " + item.attr + " of " + item.dstAttr._1).collect()
      .foreach(println)

    // 计算顶点的入度
    val degrees: VertexRDD[Int] = graph.inDegrees
    println(degrees.collect().mkString(","))

    val triplets: Array[EdgeTriplet[(String, String), String]] = graph.triplets.collect()
    triplets.foreach(item => {
      println(s"${item.srcId}  ${item.dstId} ${item.attr} ${item.dstAttr}")
    })

    val ccGraph: Graph[VertexId, String] = graph.connectedComponents()
    val value: VertexRDD[(String, String)] = graph.vertices.mapValues(item => item)

    println("-----------------")

    graph.vertices.filter(item => item._1.equals(1))
    val vertices2: VertexRDD[(String, String)] = graph.vertices
    val value1: RDD[(VertexId, (String, (String, String)))] = vertices2.map(item => (item._1, ("hello", item._2)))
    val degrees1: VertexRDD[Int] = graph.inDegrees
    val vertices3: Array[(Long, (String, String))] = Array((3L, ("rxin", "student")), (7L, ("jgonzal", "postdoc")), (5L, ("franklin", "prof")), (2L, ("istoica", "prof")))
    val users2: RDD[(Long, String)] = sc.parallelize(vertices3).map(item => (item._1,item._2._2))
    println(graph.mapEdges(item => 1L).collectEdges(EdgeDirection.Either).collect().mkString(","))
    sc.stop()
  }
}
