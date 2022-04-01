package com.cczu.spark.graphx.base

import org.apache.spark.graphx._
import org.apache.spark.graphx.util.GraphGenerators
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * pregel 迭代计算  实现最短路径
 *
 * Execute a Pregel-like iterative vertex-parallel abstraction.  The
 * user-defined vertex-program `vprog` is executed in parallel on
 * each vertex receiving any inbound messages and computing a new
 * value for the vertex.  The `sendMsg` function is then invoked on
 * all out-edges and is used to compute an optional message to the
 * destination vertex. The `mergeMsg` function is a commutative
 * associative function used to combine messages destined to the
 * same vertex.
 *
 *  执行类似 Pregel 的迭代顶点平行抽象。
 *  用户定义的顶点程序`vprog`在每个顶点上并行执行，接收任何入站消息并计算顶点的新值。
 *  `sendMsg` 函数然后在所有出边上调用，并用于计算到目标顶点的可选消息。
 *  `mergeMsg` 函数是一个可交换的关联函数，用于组合发往相同顶点的消息。
 *
 * 参数
 * initialMsg the message each vertex will receive at the on
 * the first iteration  消息初始值
 *
 * maxIterations the maximum number of iterations to run for 最大迭代次数
 *
 * activeDirection the direction of edges incident to a vertex that received a message in
 * the previous round on which to run `sendMsg`. For example, if this is `EdgeDirection.Out`, only
 * out-edges of vertices that received a message in the previous round will run.
 *  注意： 这个参数指的是，收到消息后的顶点，只有符合边的方向的才会进行下一轮迭代
 *  如果sendMsg 永远只会发送给目标节点的话，那么Out 和 Either的效果是一样的
 *
 * vprog the user-defined vertex program which runs on each
 * vertex and receives the inbound message and computes a new vertex
 * value.  On the first iteration the vertex program is invoked on
 * all vertices and is passed the default message.  On subsequent
 * iterations the vertex program is only invoked on those vertices
 * that receive messages.
 *
 * sendMsg a user supplied function that is applied to out
 * edges of vertices that received messages in the current
 * iteration
 * 用户提供一个方法用来应用于出边的点集合，并且获取到消息从当前的迭代器
 *
 * mergeMsg a user supplied function that takes two incoming
 * messages of type A and merges them into a single message of type
 * A.  ''This function must be commutative and associative and
 * ideally the size of A should not increase.''
 * 用户提供一个方法用来消费两条A类型的消息，并且合并这些消息为一个
 *
 */
object PregelGraphXDemo {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local").setAppName("graphx")
    val sc: SparkContext = new SparkContext(conf)
    val graph: Graph[Long, Double] =
      GraphGenerators.logNormalGraph(sc, numVertices = 5).mapEdges(e => e.attr.toDouble)
    val sourceId: VertexId = 2 // The ultimate source
    val initialGraph: Graph[Double, Double] = graph.mapVertices(
      // Positive infinity 正无穷大
      (id, _) => if (id == sourceId) 0.0 else Double.PositiveInfinity)
    println(Double.PositiveInfinity)
    initialGraph.vertices.collect().foreach(println)
    initialGraph.edges.collect().foreach(println)
    println("--------")
    val pregelGraph: Graph[Double, Double] = initialGraph.pregel(Double.PositiveInfinity,activeDirection = EdgeDirection.Out)(
      (vid, attr, initialMsg) => math.min(attr, initialMsg), // vertex program
      triplet => { // send message
        if (triplet.srcAttr + triplet.attr < triplet.dstAttr) {
          Iterator((triplet.dstId, triplet.srcAttr + triplet.attr))
        } else {
          Iterator.empty
        }
      },
      (a, b) => math.min(a, b) // merge message
    )
    println(pregelGraph.vertices.collect.mkString(","))
    println(pregelGraph.edges.collect.mkString(","))
    sc.stop()

  }
}
