package com.cczu.spark.graphx.base

import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * 转换操作
 * 1。 mapVertices 修改点属性
 * 2. mapEdges  修改边属性
 * 3. mapTriplets 修改边属性
 */
object TransformGraphXDemo {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local").setAppName("graphx")
    val sc: SparkContext = new SparkContext(conf)
    val vertexArray: Array[(VertexId, (String, PartitionID))] = Array(
      (1L, ("Alice", 38)),
      (2L, ("Henry", 27)),
      (3L, ("Charlie", 55)),
      (4L, ("Peter", 32)),
      (5L, ("Mike", 35)),
      (6L, ("Kate", 23))
    )
    val edgeArray: Array[Edge[PartitionID]] = Array(
      Edge(2L, 1L, 5),
      Edge(2L, 4L, 2),
      Edge(3L, 2L, 7),
      Edge(3L, 6L, 3),
      Edge(4L, 1L, 1),
      Edge(5L, 2L, 3),
      Edge(5L, 3L, 8),
      Edge(5L, 6L, 8)
    )
    val vertex: RDD[(VertexId, (String, PartitionID))] = sc.parallelize(vertexArray)
    val edge: RDD[Edge[PartitionID]] = sc.parallelize(edgeArray)
    val graph: Graph[(String, PartitionID), PartitionID] = Graph(vertex, edge)
    println("-------mapVertices-------")

    //1 mapVertices  修改实体属性
    // 使用case关键字必须用花括号
    val mapVerticesGraph: Graph[(String, PartitionID), PartitionID] = graph.mapVertices {
      case (vertexId, (name, age)) =>  (name, age + 1)
    }
    mapVerticesGraph.vertices.foreach(
    item =>{
      println(s"vid：${item._1}  attr： ${item._2}")
    })
    println("-------mapEdges-------")

    // 2 mapEdges 修改边属性
    val mapEdgesGraph: Graph[(String, PartitionID), PartitionID] = graph.mapEdges {
      edge => edge.attr * 2
    }
    mapEdgesGraph.edges.foreach(item => {
      println(s"srcId:${item.srcId} dstId:${item.dstId} attr:${item.attr}")
    })
    println("-------mapTriplets-------")
    // 3. mapTriplets 只修改边属性
    val mapTripletsGraph: Graph[(String, PartitionID), PartitionID] = graph.mapTriplets(triplets => {
      triplets.srcAttr._2 * 2
    })
    mapTripletsGraph.triplets.foreach(item => println(s"srcId:${item.srcId} strAttr:${item.srcAttr} dstId:${item.dstId} dstAttr:${item.dstAttr} edgeAttr:${item.attr}"))
    sc.stop()

  }
}

