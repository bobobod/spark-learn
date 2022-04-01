package com.cczu.spark.graphframe.pathmatcher

import java.util

import com.alibaba.fastjson.{JSON, JSONArray, JSONObject}
import org.apache.spark.SparkConf
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema
import org.apache.spark.sql.functions.{col,count,udf}
import org.apache.spark.sql.{Column, DataFrame, Dataset, Row, SparkSession}
import org.graphframes.GraphFrame

import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

/**
 * 指标计算
 */
object IndicatorComputerMain {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local").setAppName("indicatorDemo")
    val sparkSession: SparkSession = SparkSession.builder().config(conf).getOrCreate()
    val indicators: util.List[IndicatorBean] = getIndicators
    // （结构id:图结构)
    val structureMap: mutable.Map[String, StructureBean] = mutable.Map()
    for (indicator <- indicators.asScala) {
      val structures: util.List[StructureBean] = indicator.structures
      for (structure <- structures.asScala) {
        if (!structureMap.keySet.contains(structure.id)) {
          // 构建路径
          val path: String = analyzePath(structure.params)
          // 获取图结构所有的实体关系映射
          val nodeAndEdgeMap: (util.Map[String, Node], util.Map[String, Edge]) = getNodeAndEdgeMap(structure.params)
          val newStructure: StructureBean = structure.copy(path = path, nodeMap = nodeAndEdgeMap._1, edgeMap = nodeAndEdgeMap._2)
          structureMap.put(structure.id, newStructure)
        }
      }
    }
    val structureMapBc: Broadcast[mutable.Map[String, StructureBean]] = sparkSession.sparkContext.broadcast(structureMap)
    // (结构id:数据集)
    val structureData: mutable.Map[String, DataFrame] = mutable.Map()
    val graph: GraphFrame = getGraphFrame(sparkSession)
    for (structureId <- structureMap.keySet) {
      val structure: StructureBean = structureMap(structureId)
      val structureDF: Dataset[Row] = graph.find(structure.path)
        .filter(pathMatch(_, structureId))
      structureData.put(structureId, structureDF)
    }
    //    structureData.values.foreach(_.show(10))

    def pathMatch(value: Row, structureId: String): Boolean = {
      val structure: StructureBean = structureMapBc.value(structureId)
      val nodeMap: util.Map[String, Node] = structure.nodeMap
      val edgeMap: util.Map[String, Edge] = structure.edgeMap
      var matched: Boolean = true
      // 节点匹配
      for (nodeId <- nodeMap.keySet.asScala if matched) {
        val node: Node = nodeMap.get(nodeId)
        val entity: GenericRowWithSchema = value.getAs[GenericRowWithSchema](nodeId)
        val label: String = entity.getAs[String]("label")
        val props: String = entity.getAs[String]("props")
        if (!(label.equals(node.label) && propsFilterFunc(props, node.conditions))) {
          matched = false
        }
      }
      // 关系匹配
      for (edgeId <- edgeMap.keySet().asScala if matched) {
        val edge: Edge = edgeMap.get(edgeId)
        val relation: GenericRowWithSchema = value.getAs[GenericRowWithSchema](edgeId)
        val edgeLabel: String = relation.getAs[String]("edge_label")
        val edgeProps: String = relation.getAs[String]("edge_props")
        matched = edge.edgeLabelList.asScala.exists(item => item.label.equals(edgeLabel) && propsFilterFunc(edgeProps, item.conditions))
      }
      matched
    }

    // 遍历每个指标
    for (indicator <- indicators.asScala) {
      val operator: String = indicator.operator
      val masterNodeObj: JSONObject = indicator.masterNode
      val nodeListArr: JSONArray = masterNodeObj.getJSONArray("nodeList")
      val element: JSONArray = indicator.element
      // 单规则
      if (nodeListArr.size() == 1) {
        val nObject: JSONObject = nodeListArr.get(0).asInstanceOf[JSONObject]
        val structureId: String = nObject.getString("structureId")
        val masterNodeId: String = nObject.getString("nodeId")
        val selectCol1: Column = col(masterNodeId.concat(".id")).alias(masterNodeId)
        val groupCol: Column = col(masterNodeId)
        val dataFrame: DataFrame = structureData(structureId)
        operator match {
          case "count" => {
            val array: Array[AnyRef] = element.stream().map(_.asInstanceOf[JSONObject]).toArray
            val elem: JSONObject = element.get(0).asInstanceOf[JSONObject]
            val nodeId: String = elem.getString("nodeId")
            val condition: Condition = JSON.parseObject(elem.getString("conditions"), classOf[Condition])
            val computeFilterProps: String = nodeId.concat(".props")
            val computeCol: Column = col(nodeId)
            val selectCol2: Column = col(nodeId).alias(nodeId)
            val selectCols: Array[Column] = Array(selectCol1, selectCol2)
            // 先筛选
            dataFrame
              .select(selectCols: _*)
              .groupBy(groupCol)
              .agg(countUdf(condition, computeCol).head, countUdf(condition, computeCol).tail: _*)
              .show()
          }

        }
      }


    }

    sparkSession.stop()
  }



  def countUdf(condition: Condition, columns: Column*): ArrayBuffer[Column] = {
    val indicators: ArrayBuffer[Column] = new ArrayBuffer[Column]()
    for (col <- columns) {
      indicators.append(count(col))
    }
    indicators
  }

  def propsFilterFunc(props: String, condition: Condition): Boolean = {
    // 补充过滤逻辑
    true
  }

  def propsFilterFunc(props: String): Boolean = {
    // 补充过滤逻辑
    propsFilterFunc(props, null)
  }

  def analyzePath(graph: GraphBean): String = {
    val pathBuilder: StringBuilder = new mutable.StringBuilder()
    for (edge <- graph.edges.asScala) {
      pathBuilder.append(s"(${edge.srcId})-[${edge.edgeId}]->(${edge.dstId});")
    }
    val path: String = pathBuilder.mkString
    if (path.nonEmpty) {
      path.substring(0, path.lastIndexOf(";"))
    } else {
      path
    }
  }


  def getNodeAndEdgeMap(graph: GraphBean): (util.Map[String, Node], util.Map[String, Edge]) = {
    val nodeMap: util.Map[String, Node] = new util.HashMap[String, Node]()
    val edgeMap: util.Map[String, Edge] = new util.HashMap[String, Edge]()
    for (node <- graph.nodes.asScala) {
      nodeMap.put(node.id, node)
    }
    for (edge <- graph.edges.asScala) {
      edgeMap.put(edge.edgeId, edge)
    }
    (nodeMap, edgeMap)
  }



  def getGraphFrame(sparkSession: SparkSession): GraphFrame = {
    val v: DataFrame = sparkSession.createDataFrame(
      List(
        (1, "person", "{}"),
        (2, "enterprise", "{}"),
        (3, "enterprise", "{}"))
    ).toDF("id", "label", "props")
    val e: DataFrame = sparkSession.createDataFrame(
      List(
        (1, 2, "officer", "{}"),
        (1, 3, "belong", "{}"),
        (2, 3, "belong2", "{}"),
        (1, 3, "belong2", "{}")
      )
    ).toDF("src", "dst", "edge_label", "edge_props")
    GraphFrame(v, e)
  }


  def getIndicators: util.List[IndicatorBean] = {
    val str: String =
      """
        |[
        |    {
        |        "indicator": "indicator1",
        |        "isCombRule": false,
        |        "masterNode": {
        |            "nodeList": [
        |                {
        |                    "structureId": "1111",
        |                    "nodeId": "a",
        |                    "name": "自然人"
        |                }
        |            ],
        |            "label": "person"
        |        },
        |        "structures": [
        |            {
        |                "id": "1111",
        |                "params": {
        |                    "nodes": [
        |                        {
        |                            "id": "a",
        |                            "label": "person",
        |                            "showFlag": true,
        |                            "conditions": {}
        |                        },
        |                        {
        |                            "id": "b",
        |                            "label": "enterprise",
        |                            "showFlag": false,
        |                            "conditions": {}
        |                        }
        |                    ],
        |                    "edges": [
        |                        {
        |                            "edgeId": "edge1",
        |                            "srcId": "a",
        |                            "srcLabel": "person",
        |                            "dstId": "b",
        |                            "dstLabel": "enterprise",
        |                            "showFlag": true,
        |                            "edgeLabelList": [
        |                                {
        |                                    "label": "officer",
        |                                    "conditions": {}
        |                                }
        |                            ]
        |                        }
        |                    ]
        |                }
        |            }
        |        ],
        |        "element": [
        |            {
        |                "structureId": "1111",
        |                "nodeId": "b",
        |                "name": "enterprise",
        |                "label": "enterprise",
        |                "attribute": "",
        |                "conditions": {}
        |            },
        |            {
        |                "structureId": "1111",
        |                "nodeId": "b",
        |                "name": "enterprise",
        |                "label": "enterprise",
        |                "attribute": "",
        |                "conditions": {}
        |            }
        |        ],
        |        "operator": "count"
        |    }
        |]
        |""".stripMargin

    val beans: util.List[IndicatorBean] = JSON.parseArray(str, classOf[IndicatorBean])
    beans
  }

}

/**
 * 指标
 *
 * @param indicator  指标id
 * @param isCombRule 跨规则
 * @param masterNode 主实体映射关系
 * @param element    计算主体
 * @param operator   运算
 * @param structures 关联图结构集合
 */
case class IndicatorBean(
                          indicator: String,
                          isCombRule: Boolean,
                          masterNode: JSONObject,
                          element: JSONArray,
                          operator: String,
                          structures: util.List[StructureBean]
                        )

/**
 * 图结构
 *
 * @param id      图结构id
 * @param params  图结构
 * @param path    解析后的路径
 * @param nodeMap 节点映射
 * @param edgeMap 关系映射
 */
case class StructureBean(
                          id: String,
                          params: GraphBean,
                          path: String,
                          nodeMap: util.Map[String, Node],
                          edgeMap: util.Map[String, Edge]
                        )

/**
 * 图
 *
 * @param nodes
 * @param edges
 */
case class GraphBean(nodes: util.List[Node], edges: util.List[Edge])

/**
 * 节点
 *
 * @param id
 * @param label
 * @param showFlag
 * @param conditions
 */
case class Node(
                 id: String,
                 label: String,
                 showFlag: Boolean,
                 conditions: Condition
               )

/**
 * 关系
 *
 * @param edgeId
 * @param srcId
 * @param dstId
 * @param srcLabel
 * @param dstLabel
 * @param showFlag
 * @param edgeLabelList
 */
case class Edge(
                 edgeId: String,
                 srcId: String,
                 dstId: String,
                 srcLabel: String,
                 dstLabel: String,
                 showFlag: Boolean,
                 edgeLabelList: util.List[EdgeLabelBean]
               )

/**
 * 关系边类型
 *
 * @param label
 * @param conditions
 */
case class EdgeLabelBean(
                          label: String,
                          conditions: Condition
                        )

/**
 * 条件
 *
 * @param logic
 * @param group
 * @param key
 * @param operator
 * @param value
 */
case class Condition(
                      logic: String,
                      group: util.List[Condition],
                      key: String,
                      operator: String,
                      value: String
                    )

