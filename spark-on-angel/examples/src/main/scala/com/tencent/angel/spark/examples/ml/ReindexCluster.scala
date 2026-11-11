package com.tencent.angel.spark.examples.ml

import com.tencent.angel.spark.ml.core.ArgsUtil
import com.tencent.angel.spark.ml.graph.utils.GraphIO
import com.tencent.angel.spark.ml.util.DataLoader
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{LongType, StringType, StructField, StructType}
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkConf, SparkContext}

object ReindexCluster {
  def main(args: Array[String]): Unit = {
    val params = ArgsUtil.parse(args)

    val input = params.getOrElse("input", null)
    val output = params.getOrElse("output", null)
    val maps = params.getOrElse("maps", null)

    val mode = params.getOrElse("mode", "yarn")
    val isWeighted = params.getOrElse("isWeighted", "false").toBoolean
    val srcIndex = params.getOrElse("srcIndex", "0").toInt
    val dstIndex = params.getOrElse("dstIndex", "1").toInt
    val weightIndex = params.getOrElse("weightIndex", "2").toInt

    val partitionNum = params.getOrElse("partitionNum", "100").toInt
    val sep = params.getOrElse("sep",  "space") match {
      case "space" => " "
      case "comma" => ","
      case "tab" => "\t"
    }

    val sc = start(mode)
    val edgesDF = DataLoader.loadTable(sc, input, partitionNum, 1, null, sep)
    val columnsNames = edgesDF.columns


    val nodeResult =
      if (isWeighted) {
        val edges = edgesDF.select(columnsNames(srcIndex), columnsNames(dstIndex), columnsNames(weightIndex)).rdd
          .filter(row => !row.anyNull)
          .map { row => (row.get(0).toString, row.get(1).toString, row.get(2).toString) }
          .filter(f => (!f._1.isEmpty) && (!f._2.isEmpty) && (!f._3.isEmpty))
          .repartition(partitionNum).persist()

        val nodes = edges.flatMap { case (srcId, dstId, wgt) =>
          Iterator((srcId, 0.toByte), (dstId, 0.toByte))
        }.distinct().zipWithIndex()
          .map { case ((nodeId, bTag), index) =>
            (nodeId, index)
          }.persist(StorageLevel.MEMORY_ONLY)

        val leftIndex = edges.map(x => (x._1, (x._2, x._3))).leftOuterJoin(nodes).map {
          case (srcId, ((dstId, wgt), optIndex)) =>
            (dstId, (optIndex.get, wgt))
        }
        val rightIndex = leftIndex.leftOuterJoin(nodes).map {
          case (dstId, ((srcId, wgt), optIndex)) =>
            (srcId.toString, optIndex.get.toString, wgt)
        }
        val schemaWeight = StructType(Seq(
          StructField(s"${"node1"}", StringType, nullable = false),
          StructField(s"${"node2"}", StringType, nullable = false),
          StructField(s"${"weight"}", StringType, nullable = false)
        ))


        val retRDD = rightIndex.map(f => Row.fromSeq(Seq[Any](f._1, f._2, f._3)))
        val outputEdges = edgesDF.sparkSession.createDataFrame(retRDD, schemaWeight)
        GraphIO.save(outputEdges, output, sep)

        val mapCheck = nodes.map(a => (a._2, a._1))
        val dataCheck = rightIndex.map(a => (a._1.toLong, 0.toByte)).join(mapCheck).map {
          case (index, (zero, src)) => src
        }.filter(a => a.length == 16).persist()
        val dataCheckCount = dataCheck.count()
        println("wrong data count", dataCheckCount)

        if (dataCheckCount > 0) {
          val example = dataCheck.take(1)(0)
          println("example", example)
          println("ori data ", edges.filter(a => a._1 == example).collect.mkString(","))
          println("map", nodes.filter(a => a._1 == example).collect().mkString(","))

        }


        nodes

      } else {
        val edges = edgesDF.select(columnsNames(srcIndex), columnsNames(dstIndex)).rdd
          .filter(row => !row.anyNull)
          .map { row => (row.get(0).toString, row.get(1).toString) }
          .filter(f => (!f._1.isEmpty) && (!f._2.isEmpty))
          .repartition(partitionNum).persist()

        val nodes = edges.flatMap { case (srcId, dstId) =>
          Iterator((srcId, 0.toByte), (dstId, 0.toByte))
        }.distinct().zipWithIndex()
          .map { case ((nodeId, bTag), index) =>
            (nodeId, index)
          }.persist(StorageLevel.MEMORY_ONLY)

        val leftIndex = edges.map(x => (x._1, (x._2))).leftOuterJoin(nodes).map {
          case (srcId, ((dstId), optIndex)) =>
            (dstId, (optIndex.get))
        }
        val rightIndex = leftIndex.leftOuterJoin(nodes).map {
          case (dstId, ((srcId), optIndex)) =>
            (srcId.toString, optIndex.get.toString)
        }

        val schemaNoWeight = StructType(Seq(
          StructField(s"${"node1"}", StringType, nullable = false),
          StructField(s"${"node2"}", StringType, nullable = false)
        ))

        val retRDD = rightIndex.map(f => Row.fromSeq(Seq[Any](f._1, f._2)))
        val outputEdges = edgesDF.sparkSession.createDataFrame(retRDD, schemaNoWeight)
        GraphIO.save(outputEdges, output, sep)
        nodes
      }
    val schemaMap = StructType(Seq(
      StructField(s"${"oldNode"}", StringType, nullable = false),
      StructField(s"${"newNode"}", LongType, nullable = false)
    ))

    val nodeRDD = nodeResult.map(f => Row.fromSeq(Seq[Any](f._1, f._2)))
    val outputNodes = edgesDF.sparkSession.createDataFrame(nodeRDD, schemaMap)
    GraphIO.save(outputNodes, maps, sep)
  }


  def start(mode: String = "local"): SparkContext = {
    val conf = new SparkConf()
    conf.setMaster(mode)
    conf.setAppName(this.getClass.getSimpleName)
    val sc = new SparkContext(conf)
    sc.setLogLevel("WARN")
    sc
  }

  def stop(): Unit = {
    SparkContext.getOrCreate().stop()
  }


}
