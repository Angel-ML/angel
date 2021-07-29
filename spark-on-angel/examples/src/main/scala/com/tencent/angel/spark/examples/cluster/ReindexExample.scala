package com.tencent.angel.spark.examples.cluster

import java.util.{HashMap => JHashMap, HashSet => JHashSet}

import com.tencent.angel.graph.utils.GraphIO
import com.tencent.angel.spark.ml.core.ArgsUtil
import com.tencent.angel.spark.ml.util.DataLoader
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{LongType, StringType, StructField, StructType}
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.ArrayBuffer

object ReindexExample {
  def main(args: Array[String]): Unit = {
    val params = ArgsUtil.parse(args)

    val input = params.getOrElse("input", null)
    val output = params.getOrElse("output", null)
    val maps = params.getOrElse("maps", null)

    val mode = params.getOrElse("mode", "yarn-cluster")
    val isWeighted = params.getOrElse("isWeighted", "false").toBoolean
    val srcIndex = params.getOrElse("srcIndex", "0").toInt
    val dstIndex = params.getOrElse("dstIndex", "1").toInt
    val weightIndex = params.getOrElse("weightIndex", "2").toInt

    val partitionNum = params.getOrElse("partitionNum", "100").toInt
    val sep = params.getOrElse("sep", "space") match {
      case "space" => " "
      case "comma" => ","
      case "tab" => "\t"
    }

    val sc = start(mode)
    val edgesDF = DataLoader.loadTable(sc, input, partitionNum, 1, null, sep)
    val columnsNames = edgesDF.columns

    val edgesRDD = if (isWeighted) {
      edgesDF.select(columnsNames(srcIndex), columnsNames(dstIndex), columnsNames(weightIndex)).rdd
        .filter(row => !row.anyNull)
        .map { row => Array(row.get(0).toString, row.get(1).toString, row.get(2).toString) }
        .filter(f => (!f(0).isEmpty) && (!f(1).isEmpty) && (!f(2).isEmpty))
    }
    else {
      edgesDF.select(columnsNames(srcIndex), columnsNames(dstIndex)).rdd
        .filter(row => !row.anyNull)
        .map { row => Array(row.get(0).toString, row.get(1).toString) }
        .filter(f => (!f(0).isEmpty) && (!f(1).isEmpty))
    }

    val nodes = edgesRDD.flatMap { f => Iterator((f(0), 1), (f(1), 1)) }.reduceByKey(_ + _).map(f => f._1)
    val nodeWithIndex = nodes.zipWithIndex().map(ele => (ele._1, ele._2)).cache()

    def buildRoutingTable(index: Int, iterator: Iterator[Array[String]]): Iterator[(String, Int)] = {
      val set = new JHashSet[String]()
      while (iterator.hasNext) {
        val line = iterator.next()
        set.add(line(0))
        set.add(line(1))
      }

      val it = set.iterator()
      val result = new ArrayBuffer[(String, Int)]()
      while (it.hasNext) {
        result.append((it.next(), index))
      }

      result.iterator
    }

    val routingTable = edgesRDD.mapPartitionsWithIndex((partId, iterator) =>
      buildRoutingTable(partId, iterator), true)

    val partIndex = routingTable.join(nodeWithIndex).map { case (str, (partId, index)) =>
      (partId, (str, index))
    }.groupByKey(edgesRDD.getNumPartitions)


    def attachPartitionId(index: Int, iterator: Iterator[Array[String]]): Iterator[(Int, Array[Array[String]])] = {
      Iterator.single((index, iterator.toArray))
    }

    val edgesNew = edgesRDD.mapPartitionsWithIndex((partId, iterator) =>
      attachPartitionId(partId, iterator), true)
      .join(partIndex).map { case (_, (edgesTmp, mapping)) =>
      val map = new JHashMap[String, Long]()
      for ((string, index) <- mapping) {
        map.put(string, index)
      }

      if (isWeighted) {
        edgesTmp.map(line => Array(map.get(line(0)).toString, map.get(line(1)).toString, line(2)))
      }
      else {
        edgesTmp.map(line => Array(map.get(line(0)).toString, map.get(line(1)).toString))
      }
    }.flatMap(f => f)


    if (isWeighted) {
      val schemaWeight = StructType(Seq(
        StructField(s"${"node1"}", StringType, nullable = false),
        StructField(s"${"node2"}", StringType, nullable = false),
        StructField(s"${"weight"}", StringType, nullable = false)
      ))

      val retRDD = edgesNew.map(f => Row.fromSeq(Seq[Any](f(0), f(1), f(2))))
      val outputEdges = edgesDF.sparkSession.createDataFrame(retRDD, schemaWeight)
      GraphIO.save(outputEdges, output, sep)

    }
    else {
      val schemaNoWeight = StructType(Seq(
        StructField(s"${"node1"}", StringType, nullable = false),
        StructField(s"${"node2"}", StringType, nullable = false)
      ))

      val retRDD = edgesNew.map(f => Row.fromSeq(Seq[Any](f(0), f(1))))
      val outputEdges = edgesDF.sparkSession.createDataFrame(retRDD, schemaNoWeight)
      GraphIO.save(outputEdges, output, sep)
    }

    val schemaMap = StructType(Seq(
      StructField(s"${"oldNode"}", StringType, nullable = false),
      StructField(s"${"newNode"}", LongType, nullable = false)
    ))
    val nodeRDD = nodeWithIndex.map(f => Row.fromSeq(Seq[Any](f._1, f._2)))
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
