package com.cczu.spark.graphx.base

import org.apache.spark.graphx._
import org.apache.spark.graphx.util.GraphGenerators
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * 聚合操作
 * aggregateMessages
 * collectNeighbors
 * collectNeighborIds
 */
object AggregationGraphXDemo {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local").setAppName("graphx")
    val sc: SparkContext = new SparkContext(conf)
    val graph: Graph[Double, PartitionID] = GraphGenerators.logNormalGraph(sc, numVertices = 100).mapVertices((vid, _) => vid.toDouble)
    print("---aggregateMessages----")
    //   下面的例子计算比用户年龄大的追随者（即followers）的平均年龄。
    // a--follower-->b a 是跟随者 b是被跟随者  olderFollowers (vid,(总比自己大follower人数，总比自己大的年龄和))
    val olderFollowers: VertexRDD[(PartitionID, Double)] = graph.aggregateMessages[(PartitionID, Double)](
      // map func
      triplet => {
        if (triplet.srcAttr > triplet.dstAttr) {
          // 发送消息给被跟随者
          triplet.sendToDst(1, triplet.srcAttr)
        }
      },
      // reduce func  合并消息
      (a, b) => (a._1 + b._1, a._2 + b._2)
    )
    olderFollowers.mapValues((vid, value) => value match {
      case (cnt, age) => age / cnt
    }).collect().foreach(println)

    print("----collectNeighbors----")
    // collectNeighbors 会找出所有的邻居节点会附带属性，邻居节点可能会重复
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
    val graph2: Graph[(String, String), String] = Graph(users, relationships, defaultUser)
    val neighbors: VertexRDD[Array[(VertexId, (String, String))]] = graph2.collectNeighbors(EdgeDirection.Either)
    neighbors.collect().foreach(item => {
      println(item._1, item._2.mkString(","))
    })
    print("-------collectNeighborIds-----")
    // EdgeDirection
    //  Either  入边的点或者出边的点收到消息后，才继续发送消息
    //  BOTH    入边的点和出边的点都收到消息后，才继续发送消息
    //   In     只有入边的点收到消息后，才继续发送消息
    //   Out    只有出边的点收到消息后，才继续发送消息
    val neighborIds: VertexRDD[Array[VertexId]] = graph2.collectNeighborIds(EdgeDirection.Either)
    neighborIds.collect().foreach(item => {
      println(item._1, item._2.mkString(","))
    })

    sc.stop()

  }
}
