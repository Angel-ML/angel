/*
 * Tencent is pleased to support the open source community by making Angel available.
 *
 * Copyright (C) 2017-2018 THL A29 Limited, a Tencent company. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in
 * compliance with the License. You may obtain a copy of the License at
 *
 * https://opensource.org/licenses/Apache-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 *
 */
package com.tencent.angel.spark.ml.graph.utils.partitionstrategy.reindexpartition

import com.tencent.angel.spark.ml.graph.utils.params._
import org.apache.spark.SparkContext
import org.apache.spark.ml.Transformer
import org.apache.spark.ml.param.ParamMap
import org.apache.spark.ml.util.Identifiable
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Dataset, Row}
import org.apache.spark.storage.StorageLevel

import scala.collection.mutable.ArrayBuffer

// arrange each partition with similar number of edges
// save the arrayBounds for rangePartitioner
class RePartition(override val uid: String) extends Transformer
  with HasSrcNodeIdCol with HasDstNodeIdCol with HasOutputNodeIdCol
  with HasPartitionNum with HasStorageLevel with HasUseEdgeBalancePartition {

  def this() = this(Identifiable.randomUID("ReIndex"))

  override def transform(dataset: Dataset[_]): DataFrame = {
    //read edges
    val edges = dataset.select($(srcNodeIdCol), $(dstNodeIdCol)).rdd
      .filter(row => !row.anyNull)
      .map(row => (row.getLong(0), row.getLong(1)))
      .flatMap(f => Iterator((f._1, f._2), (f._2, f._1)))
    //edges's storageLevel choose  Disk_Only
    edges.persist(StorageLevel.DISK_ONLY)

    //    val nodes = edges.flatMap(e => Iterator(e._1, e._2)).distinct()
    val maxId = edges.map(e => math.max(e._1, e._2)).max()
    val minId = edges.map(e => math.min(e._1, e._2)).min()
    val numEdges = edges.count()
    //    val numNodes = nodes.count()

    println(s"minId=$minId maxId=$maxId numEdges=$numEdges level=${$(storageLevel)}")

    val degree_ = edges.groupByKey().map(x => (x._1, x._2.size))
    val degree = degree_.sortBy(_._1)
    val maxDegree = degree.map(_._2).max()

    val alpha = numEdges / $(partitionNum) + 1
    val numEdgesForOnePartition = if (alpha > maxDegree) alpha else maxDegree

    println(s"len of degree: ${degree.count()}, sum of degree: ${degree.map(_._2).sum}, maxDegree: $maxDegree, alpha: $alpha, numEdgesForOnePartition: $numEdgesForOnePartition")

    def accumulateWithinPartition(index: Int, iter: Iterator[(Long, Int)]): (Int, Array[(Long, Long)], Long) = {
      val re = ArrayBuffer[(Long, Long)]()
      var acc = 0L
      while (iter.hasNext) {
        val (nodeId, d) = iter.next()
        acc += d
        re += ((nodeId, acc))
      }
      (index, re.toArray, acc)
}

    val xx = degree.mapPartitionsWithIndex((index, it) => Iterator(accumulateWithinPartition(index, it)))
    val sumsOfDegreeParts = xx.map(x => (x._1, x._3)).collectAsMap()
    var add = scala.collection.mutable.Map(0 -> 0L)
    var acc = sumsOfDegreeParts(0)
    for (sortedPartIndex <- 1 until sumsOfDegreeParts.size) {
      val temp = sumsOfDegreeParts(sortedPartIndex)
      add += ((sortedPartIndex, acc))
      acc += temp
    }
    assert(acc == degree.map(_._2).sum, s"acc is not equal to sum of degrees: $acc vs ${degree.map(_._2).sum}")
//    val addNumsForDegreeParts = sumsOfDegreeParts.map{ case (index, dee) =>
//      acc += dee
//      (index, acc)
//    }
    val xxx = xx.map{ case (index, dees, sum) => dees.map(x => (x._1, x._2 + add(index)))}.flatMap(x=>x)
    val nodesToPartitionId_ = xxx.map{ case (nodeId, accDee) => (accDee / numEdgesForOnePartition, nodeId)}
        .groupByKey().map(x => (x._1, x._2.max)).collect().sortBy(_._1)

    val nodesToPartitionId = nodesToPartitionId_.slice(0, nodesToPartitionId_.length - 1)
    val arrayBounds = if (nodesToPartitionId.length < 1) Array[Long]() else {
      val lastNode = nodesToPartitionId(nodesToPartitionId.length - 1)._2
      val lastDee = degree.filter(x => x._1 > lastNode).map(_._2).sum
      println(s"last dee: $lastDee; percentage to numEdgesForOnePartition: ${lastDee.toFloat / numEdgesForOnePartition}")
      if (lastDee < numEdgesForOnePartition * 0.5) {
        if (nodesToPartitionId.length > 0) {
          nodesToPartitionId.slice(0, nodesToPartitionId.length - 1).map(_._2)
        } else {
          Array[Long]()
        }
      } else {
        nodesToPartitionId.map(_._2)
      }
    }
    println(s"arrayBounds:")
    arrayBounds.foreach(println)

    def variance(in: Array[Int]): (Float, Float) = {
      val average = in.map(_.toLong).sum / in.length
      val variance = in.map(a=> (a-average)*(a-average)).sum
      (average, variance)
    }

    if ($(useEdgeBalancePartition)) {
      val beforePartition = edges.mapPartitionsWithIndex((num, it) => Iterator(it.size)).collect()
      val (a, v) = variance(beforePartition)
      println(s"read partitions: ${beforePartition.length}, average and variance of numEdges are: $a and $v ")

      val balancedPartitionNum = arrayBounds.length + 1
      val rePartitioned = edges.repartitionAndSortWithinPartitions(new MyPartition(balancedPartitionNum, arrayBounds))
      val data = rePartitioned.mapPartitions(iter => Iterator(iter.size)).collect()
      val (a_, v_) = variance(data)
      println(s"graph partitions: ${rePartitioned.getNumPartitions}, average and variance of numEdges are: $a_ and $v_ ")

    }

    val retRDD = SparkContext.getOrCreate()
      .parallelize(arrayBounds.toSeq, math.max(1, math.min($(partitionNum), arrayBounds.length)))
      .map(r => Row.fromSeq(Seq[Any](r)))
    dataset.sparkSession.createDataFrame(retRDD, transformSchema(dataset.schema))
  }


  override def transformSchema(schema: StructType): StructType = {
    StructType(Seq(
      StructField(s"${
        $(outputNodeIdCol)
      }", LongType, nullable = false)
    ))
  }

  override def copy(extra: ParamMap): Transformer = defaultCopy(extra)

}