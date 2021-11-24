package com.cczu.spark.graphframe

import org.apache.spark.graphx.{Edge, EdgeRDD, Graph}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}

object GraphFrameDemo {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local").setAppName("graphx")
    val sparkSession: SparkSession = SparkSession.builder().config(conf).getOrCreate()
    val v: DataFrame = sparkSession.createDataFrame(
      List((1, "alice", 23), (2, "bob", 33), (3, "smith", 44))
    ).toDF("id", "name", "age")
    val e: DataFrame = sparkSession.createDataFrame(List(
      (1, 2, "friend","props"),
      (2, 3, "follow","props"),
      (1, 3, "follow","props")
    )).toDF("src", "dst", "relationship","props")
    import org.graphframes.GraphFrame
     val graph: GraphFrame = GraphFrame(v, e)
    graph.degrees.show()
    graph.vertices.show()
    graph.edges.show()
    import sparkSession.implicits._

    val graphX: Graph[Row, Row] = graph.toGraphX
    val reverse: EdgeRDD[Row] = graphX.edges.reverse
    val value: RDD[Edge[Row]] = graphX.edges.union(reverse)
    val graph2: GraphFrame = GraphFrame.fromGraphX(Graph(graphX.vertices, value))
    val dataFrame: DataFrame = graph2.find("(a)-[e]->(b);(b)-[e2]->(c)").where(
      "e.relationship = 'friend' and e2.relationship = 'follow' "
    ).select("a", "b", "c").withColumn("group_id",$"a.id")
    dataFrame.show()
    sparkSession.close()
  }
}
