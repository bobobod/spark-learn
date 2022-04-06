package com.cczu.spark.graphframe.pathmatcher

import java.util

import com.alibaba.fastjson.{JSON, JSONObject}
import com.alibaba.fastjson.serializer.SerializerFeature
import org.apache.spark.SparkConf
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions._
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

    structureData.values.foreach(_.show())

    val propsFilterUdf: UserDefinedFunction = udf(propsFilterFunc(_: String, _: String))
    // 遍历每个指标
    for (indicator <- indicators.asScala) {
      val operator: String = indicator.operator
      val masterNodes: util.List[MasterNodeBean] = indicator.masterNodes
      val computeNodes: util.List[ComputeNodeBean] = indicator.computeNodes
      // 单规则
      if (!indicator.isCombRule) {
        // 取第一个主节点
        val masterNode: MasterNodeBean = masterNodes.get(0)
        val structureId: String = masterNode.structureId
        val masterNodeId: String = masterNode.nodeId
        val groupCol: Column = col(masterNodeId.concat(".id")).alias(masterNodeId)
        val dataFrame: DataFrame = structureData(structureId)

        dataFrame
          .groupBy(groupCol)
          .agg(aggCompute(computeNodes, operator, propsFilterUdf).head,
            aggCompute(computeNodes, operator, propsFilterUdf).tail: _*)
          .show()
      }
    }

    sparkSession.stop()
  }


  def aggCompute(computeNodes: util.List[ComputeNodeBean], operator: String, propsFilterUdf: UserDefinedFunction): ArrayBuffer[Column] = {
    val cols: ArrayBuffer[Column] = new ArrayBuffer[Column]()

    computeNodes.asScala.foreach(computeNode => {
      val condition: CustomCondition = computeNode.condition
      val attribute: String = computeNode.attribute
      val computeCol: Column = col(computeNode.nodeId)

      val aggOper: Column = operator match {
        case "count" => {
          // 属性过滤
          val col: Column = when(
            propsFilterUdf(computeCol.getField("props"), lit(JSON.toJSONString(condition, SerializerFeature.MapSortField))),
            computeCol.getField("id"))
            .otherwise(lit(null))
          // 去重统计
          countDistinct(col).alias(computeNode.nodeId)
        }
        case "attributeMax" => {
          val col: Column = when(
            propsFilterUdf(computeCol.getField("props"), lit(JSON.toJSONString(condition, SerializerFeature.MapSortField))),
            get_json_object(computeCol.getField("props"), "$.".concat(attribute)))
            .otherwise(lit(null)
            )
          max(col).alias(computeNode.nodeId)
        }
        case "attributeMin" => {
          val col: Column = when(
            propsFilterUdf(computeCol.getField("props"), lit(JSON.toJSONString(condition, SerializerFeature.MapSortField))),
            get_json_object(computeCol.getField("props"), "$.".concat(attribute)))
            .otherwise(lit(null)
            )
          min(col).alias(computeNode.nodeId)
        }
        case "attributeAvg" => {
          val col: Column = when(
            propsFilterUdf(computeCol.getField("props"), lit(JSON.toJSONString(condition, SerializerFeature.MapSortField))),
            get_json_object(computeCol.getField("props"), "$.".concat(attribute)))
            .otherwise(lit(null)
            )
          avg(col).alias(computeNode.nodeId)
        }
        case "attributeMean" => {
          val col: Column = when(
            propsFilterUdf(computeCol.getField("props"), lit(JSON.toJSONString(condition, SerializerFeature.MapSortField))),
            get_json_object(computeCol.getField("props"), "$.".concat(attribute)))
            .otherwise(lit(null)
            )
          mean(col).alias(computeNode.nodeId)
        }
        case "attributeVariance" => {
          val col: Column = when(
            propsFilterUdf(computeCol.getField("props"), lit(JSON.toJSONString(condition, SerializerFeature.MapSortField))),
            get_json_object(computeCol.getField("props"), "$.".concat(attribute)))
            .otherwise(lit(null))
          variance(col).alias(computeNode.nodeId)
        }

      }

      cols.append(aggOper)
    })
    cols
  }


  def propsFilterFunc(props: String, condition: CustomCondition): Boolean = {
    // 补充过滤逻辑
    true
  }

  /**
   * 自定义属性过滤udf
   *
   * @param props
   * @param conditionStr
   * @return
   */
  def propsFilterFunc(props: String, conditionStr: String): Boolean = {
    val condition: CustomCondition = JSON.parseObject(conditionStr, classOf[CustomCondition])
    propsFilterFunc(props, condition)
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
        (2, "enterprise", "{\"money\":111}"),
        (3, "enterprise", "{\"money\":222}"),
        (4, "enterprise", "{\"money\":333}")
      )
    ).toDF("id", "label", "props")
    val e: DataFrame = sparkSession.createDataFrame(
      List(
        (1, 2, "officer", "{}"),
        (1, 3, "belong", "{}"),
        (2, 3, "belong3", "{}"),
        (1, 3, "belong2", "{}"),
        (1, 4, "belong2", "{}")
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
        |        "indicatorName":"指标名",
        |        "isCombRule": false,
        |        "masterNodes": [
        |          {
        |                 "structureId": "1111",
        |                 "nodeId": "a",
        |                 "name": "自然人",
        |                 "label": "person"
        |           }
        |        ],
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
        |                        },
        |                        {
        |                            "id": "c",
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
        |                        },
        |                        {
        |                            "edgeId": "edge2",
        |                            "srcId": "a",
        |                            "srcLabel": "person",
        |                            "dstId": "c",
        |                            "dstLabel": "enterprise",
        |                            "showFlag": true,
        |                            "edgeLabelList": [
        |                                {
        |                                    "label": "belong",
        |                                    "conditions": {}
        |                                },
        |                                {
        |                                    "label": "belong2",
        |                                    "conditions": {}
        |                                }
        |                            ]
        |                        }
        |                    ]
        |                }
        |            }
        |        ],
        |        "computeNodes": [
        |            {
        |                "structureId": "1111",
        |                "nodeId": "b",
        |                "name": "enterprise",
        |                "label": "enterprise",
        |                "attribute": "money",
        |                "ratioType":"",
        |                "conditions": {}
        |            },
        |            {
        |                "structureId": "1111",
        |                "nodeId": "c",
        |                "name": "enterprise",
        |                "label": "enterprise",
        |                "attribute": "money",
        |                "ratioType":"",
        |                "conditions": {}
        |            }
        |        ],
        |        "operator": "attributeMax"
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
 * @param indicator     指标id
 * @param indicatorName 指标名称
 * @param isCombRule    跨规则
 * @param masterNodes   主实体映射关系
 * @param computeNodes  计算主体
 * @param operator      运算
 * @param structures    关联图结构集合
 */
case class IndicatorBean(
                          indicator: String,
                          indicatorName: String,
                          isCombRule: Boolean,
                          masterNodes: util.List[MasterNodeBean],
                          computeNodes: util.List[ComputeNodeBean],
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
                 conditions: CustomCondition
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
                          conditions: CustomCondition
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
                      group: util.List[CustomCondition],
                      key: String,
                      operator: String,
                      value: String
                    )

/**
 * 计算节点配置
 *
 * @param structureId
 * @param nodeId
 * @param name
 * @param label
 * @param attribute
 * @param ratioType
 * @param condition
 */
case class ComputeNodeBean(
                            structureId: String,
                            nodeId: String,
                            name: String,
                            label: String,
                            attribute: String,
                            ratioType: String,
                            condition: CustomCondition
                          )

/**
 * 主节点
 *
 * @param structureId
 * @param nodeId
 * @param name
 * @param label
 */
case class MasterNodeBean(
                           structureId: String,
                           nodeId: String,
                           name: String,
                           label: String
                         )

