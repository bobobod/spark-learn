package com.cczu.spark.graphx

import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object GraphXDemo2 {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local").setAppName("graphx")
    val sc: SparkContext = new SparkContext(conf)
    val vertexArray = Array(
      (1L, ("Alice", 38)),
      (2L, ("Henry", 27)),
      (3L, ("Charlie", 55)),
      (4L, ("Peter", 32)),
      (5L, ("Mike", 35)),
      (6L, ("Kate", 23))
    )
    val edgeArray = Array(
      Edge(2L, 1L, 5),
      Edge(2L, 4L, 2),
      Edge(3L, 2L, 7),
      Edge(3L, 6L, 3),
      Edge(4L, 1L, 1),
      Edge(5L, 2L, 3),
      Edge(5L, 3L, 8),
      Edge(5L, 6L, 8)
    )
    val vertices: RDD[(VertexId, (String, PartitionID))] = sc.parallelize(vertexArray)
    val edges: RDD[Edge[PartitionID]] = sc.parallelize(edgeArray)
    val graph: Graph[(String, PartitionID), PartitionID] = Graph(vertices, edges)
    graph.vertices
    graph.edges
    graph.triplets
    graph.degrees
    graph.inDegrees
    graph.outDegrees
    // 1. 找出年龄大于30的顶点
    println()
    graph.vertices.filter { case (id, (name, age)) => age > 30 }.collect().foreach { case (id, (name, age)) => println(s"$name is $age") }
    // 2。找出边属性大于3的边
    println()
    graph.edges.filter(item => item.attr > 3).collect().foreach(println)

    // 3. 找出图中最大的度数 图有几度
    def max(a: (VertexId, Int), b: (VertexId, Int)): (VertexId, Int) = {
      if (a._2 > b._2) a else b
    }

    println()
    println(graph.degrees.reduce(max))
    println(graph.inDegrees.reduce(max))
    println(graph.outDegrees.reduce(max))
    // 4. 顶点转换 age+10
    println()
    graph.mapVertices { case (id, (name, age)) => (id, (name, age + 10)) }.vertices.collect().foreach(println)
    // 5. 边转换
    println()
    graph.mapEdges(e => e.attr * 2).edges.collect().foreach(println)
    // 6.找出顶点年纪>30的子图
    //  def subgraph(
    //      epred: EdgeTriplet[VD,ED] => Boolean = (x => true),
    //      vpred: (VertexId, VD) => Boolean = ((v, d) => true))
    //    : Graph[VD, ED]
    println()
    val subGraph: Graph[(String, PartitionID), PartitionID] = graph.subgraph(vpred = (id, vd) => vd._2 > 30)
    // 子图所有点
    subGraph.vertices
    // 子图所有边
    subGraph.edges

    // 7.找出每个实体 年纪最小的follow leftjoin 是join VerticeId
    val youngest: VertexRDD[(String, PartitionID)] = graph.aggregateMessages[(String, PartitionID)](triple => {
      triple.sendToDst(triple.srcAttr._1, triple.srcAttr._2)
    }, (a, b) => if (a._2 > b._2) b else a)
    println()
    for (elem <- youngest.collect()) {
      println(elem._1, elem._2)
    }

    println(
    )
    graph.vertices.leftJoin(youngest) {
      (id, user, optOldestFollower) => {
        optOldestFollower match {
          case Some((name, age)) => s"$id ${name} is the youngest follower of ${user._1}";
          case None => s"${user._1} has no follower"
        }
      }
    }.collect().foreach(println)
    sc.stop()
  }
}
