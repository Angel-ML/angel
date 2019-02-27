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


package com.tencent.angel.spark.ml.tree.gbdt.dataset

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

import com.tencent.angel.spark.ml.tree.gbdt.metadata.FeatureInfo
import com.tencent.angel.spark.ml.tree.sketch.HeapQuantileSketch
import com.tencent.angel.spark.ml.tree.util.Maths

import scala.collection.mutable.{ArrayBuilder => AB}
import scala.io.Source
import java.{util => ju}

object Dataset extends Serializable {

  def fromDisk(path: String, dim: Int): Dataset[Int, Float] = {
    val labels = new AB.ofFloat
    val indices = new AB.ofInt
    val values = new AB.ofFloat
    val indexEnds = new AB.ofInt
    var curIndex = 0
    labels.sizeHint(1 << 20)
    indices.sizeHint(1 << 20)
    values.sizeHint(1 << 20)
    indexEnds.sizeHint(1 << 20)

    val reader = Source.fromFile(path).bufferedReader()
    var line = reader.readLine()
    while (line != null) {
      line = line.trim
      if (line.nonEmpty && !line.startsWith("#")) {
        val splits = line.split("\\s+|,").map(_.trim)
        labels += splits(0).toFloat
        for (i <- 0 until splits.length - 1) {
          val kv = splits(i + 1).split(":")
          indices += kv(0).toInt
          values += kv(1).toFloat
          curIndex += 1
        }
        indexEnds += curIndex
      }
      line = reader.readLine()
    }

    Dataset(Array(new LabeledPartition(labels.result(),
      indices.result(), values.result(), indexEnds.result()))
    )
  }

  def fromTextFile(path: String, dim: Int, numWorker: Int)
                  (implicit sc: SparkContext): RDD[LabeledPartition[Int, Float]] = {
    sc.textFile(path).repartition(numWorker).mapPartitions(iterator => {
      val labels = new AB.ofFloat
      val indices = new AB.ofInt
      val values = new AB.ofFloat
      val indexEnds = new AB.ofInt
      var curIndex = 0
      labels.sizeHint(1 << 20)
      indices.sizeHint(1 << 20)
      values.sizeHint(1 << 20)
      indexEnds.sizeHint(1 << 20)
      iterator.foreach(text => {
        val line = text.trim
        if (line.nonEmpty && !line.startsWith("#")) {
          val splits = line.split("\\s+|,").map(_.trim)
          labels += splits(0).toFloat
          for (i <- 0 until splits.length - 1) {
            val kv = splits(i + 1).split(":")
            indices += kv(0).toInt
            values += kv(1).toFloat
            curIndex += 1
          }
          indexEnds += curIndex
        }
      })
      Iterator(new LabeledPartition(labels.result(),
        indices.result(), values.result(), indexEnds.result()))
    })
  }

  def createSketches(dataset: Dataset[Int, Float], dim: Int): Array[HeapQuantileSketch] = {
    val sketches = new Array[HeapQuantileSketch](dim)
    for (i <- 0 until dim)
      sketches(i) = new HeapQuantileSketch()
    dataset.partitions.foreach(partition => {
      val numKV = partition.numKVPair
      val indices = partition.indices
      val values = partition.values
      for (i <- 0 until numKV)
        sketches(indices(i)).update(values(i))
    })
    sketches
  }

  def binning(dataset: Dataset[Int, Float], featureInfo: FeatureInfo): Dataset[Int, Int] = {
    val res = Dataset[Int, Int](dataset.numPartition, dataset.numInstance)
    for (partId <- 0 until dataset.numPartition) {
      val indices = dataset.partitions(partId).indices
      val values = dataset.partitions(partId).values
      val bins = Array.ofDim[Int](indices.length)
      for (i <- indices.indices) {
        bins(i) = Maths.indexOf(featureInfo.getSplits(indices(i)), values(i))
      }
      val indexEnds = dataset.partitions(partId).indexEnds
      res.appendPartition(indices, bins, indexEnds)
    }
    res
  }

  def columnGrouping(dataset: Dataset[Int, Float], fidToGroupId: Array[Int],
                     fidToNewFid: Array[Int], featureInfo: FeatureInfo,
                     numGroup: Int): Array[Partition[Short, Byte]] = {
    val size = dataset.size
    val numKV = dataset.numKVPair
    val indices = new Array[AB.ofShort](numGroup)
    val bins = new Array[AB.ofByte](numGroup)
    val indexEnds = new Array[AB.ofInt](numGroup)
    val curIndex = new Array[Int](numGroup)
    for (groupId <- 0 until numGroup) {
      indices(groupId) = new AB.ofShort
      bins(groupId) = new AB.ofByte
      indexEnds(groupId) = new AB.ofInt
      indices(groupId).sizeHint((1.2 * numKV / numGroup).toInt)
      bins(groupId).sizeHint((1.2 * numKV / numGroup).toInt)
      indexEnds(groupId).sizeHint((1.2 * size / numGroup).toInt)
    }

    dataset.partitions.foreach(partition => {
      val size = partition.size
      val partIndices = partition.indices
      val partValues = partition.values
      val partIndexEnds = partition.indexEnds
      var indexStart = 0
      for (i <- 0 until size) {
        for (j <- indexStart until partIndexEnds(i)) {
          val fid = partIndices(j)
          val fvalue = partValues(j)
          val groupId = fidToGroupId(fid)
          val newFid = fidToNewFid(fid)
          val binId = Maths.indexOf(featureInfo.getSplits(fid), fvalue)
          indices(groupId) += (newFid + Short.MinValue).toShort
          bins(groupId) += (binId + Byte.MinValue).toByte
          curIndex(groupId) += 1
        }
        indexStart = partIndexEnds(i)
        for (groupId <- 0 until numGroup) {
          indexEnds(groupId) += curIndex(groupId)
        }
      }
    })

    (indices, bins, indexEnds).zipped.map {
      case (groupIndices, groupBins, groupIndexEnds) =>
        new Partition[Short, Byte](groupIndices.result(),
          groupBins.result(), groupIndexEnds.result())
    }
  }

  def merge(partitions: Array[Partition[Short, Byte]]): Dataset[Int, Int] = {
    val numPartition = partitions.length
    val numInstance = partitions.map(_.size).sum
    val res = Dataset[Int, Int](numPartition, numInstance)
    partitions.foreach(partition => {
      val indices = partition.indices.map(_.toInt - Short.MinValue)
      val bins = partition.values.map(_.toInt - Byte.MinValue)
      res.appendPartition(indices, bins, partition.indexEnds)
    })
    res
  }

  def restore(dataset: Dataset[Short, Byte]): Dataset[Int, Int] = {
    val res = new Dataset[Int, Int](dataset.numPartition, dataset.numInstance)
    dataset.partitions.foreach(partition => {
      val numKV = partition.numKVPair
      val indices = new Array[Int](numKV)
      val bins = new Array[Int](numKV)
      val shortIndices = partition.indices
      val byteBins = partition.values
      for (i <- 0 until numKV) {
        indices(i) = shortIndices(i).toInt - Short.MinValue
        bins(i) = byteBins(i).toInt - Byte.MinValue
      }
      res.appendPartition(indices, bins, partition.indexEnds)
    })
    res
  }

  def apply[@specialized(Byte, Short, Int, Long, Float, Double) K,
  @specialized(Byte, Short, Int, Long, Float, Double) V]
  (maxNumPartition: Int, maxNumInstance: Int): Dataset[K, V] =
    new Dataset(maxNumPartition, maxNumInstance)

  def apply[@specialized(Byte, Short, Int, Long, Float, Double) K,
  @specialized(Byte, Short, Int, Long, Float, Double) V]
  (partitions: Seq[Partition[K, V]]): Dataset[K, V] = {
    val numPartition = partitions.length
    val numInstance = partitions.map(_.size).sum
    val res = new Dataset[K, V](numPartition, numInstance)
    partitions.foreach(res.appendPartition)
    res
  }

}

private[gbdt] case class Partition
[@specialized(Byte, Short, Int, Long, Float, Double) K,
@specialized(Byte, Short, Int, Long, Float, Double) V]
(indices: Array[K], values: Array[V], indexEnds: Array[Int]) {

  def size: Int = indexEnds.length

  def numKVPair: Int = indices.length
}

private[gbdt] class LabeledPartition
[@specialized(Byte, Short, Int, Long, Float, Double) K,
@specialized(Byte, Short, Int, Long, Float, Double) V]
(val labels: Array[Float], _indices: Array[K], _values: Array[V], _indexEnds: Array[Int])
  extends Partition[K, V](_indices, _values, _indexEnds)

class Dataset[@specialized(Byte, Short, Int, Long, Float, Double) K,
@specialized(Byte, Short, Int, Long, Float, Double) V](maxNumPartition: Int, maxNumInstance: Int)
  extends Serializable {
  private[gbdt] val partitions = new Array[Partition[K, V]](maxNumPartition)
  @transient private[gbdt] val partOffsets = new Array[Int](maxNumPartition)
  @transient private[gbdt] val insLayouts = new Array[Int](maxNumInstance)
  private[gbdt] var numPartition = 0
  private[gbdt] var numInstance = 0

  def appendPartition(partition: Partition[K, V]): Unit = {
    require(numPartition < maxNumPartition && numInstance + partition.size <= maxNumInstance)
    val partId = numPartition
    partitions(partId) = partition
    if (partId == 0) {
      partOffsets(partId) = 0
    } else {
      partOffsets(partId) = partOffsets(partId - 1) + partitions(partId - 1).size
    }
    for (i <- 0 until partition.size)
      insLayouts(partOffsets(partId) + i) = partId
    numPartition += 1
    numInstance += partition.size
  }

  def appendPartition(indices: Array[K], values: Array[V], indexEnds: Array[Int]): Unit =
    appendPartition(new Partition[K, V](indices, values, indexEnds))

  def get(insId: Int, fid: Int): V = {
    val partId = insLayouts(insId)
    val partition = partitions(partId).asInstanceOf[Partition[Int, V]]
    val partInsId = insId - partOffsets(partId)
    val start = if (partInsId == 0) 0 else partition.indexEnds(partInsId - 1)
    val end = partition.indexEnds(partInsId)
    val t = ju.Arrays.binarySearch(partition.indices, start, end, fid)
    if (t >= 0) partition.values(t) else (-1).asInstanceOf[V]
  }

  def size: Int = numInstance

  def numKVPair: Int = partitions.map(_.numKVPair).sum

  def getLabels: Option[Array[Float]] = {
    if (numPartition == 0) {
      None
    } else {
      partitions(0) match {
        case _: LabeledPartition[K, V] =>
          if (numPartition == 1) {
            Option(partitions(0).asInstanceOf[LabeledPartition[K, V]].labels)
          } else {
            val labels = new Array[Float](size)
            var offset = 0
            for (partId <- 0 until numPartition) {
              val partLabels = partitions(partId)
                .asInstanceOf[LabeledPartition[K, V]].labels
              Array.copy(partLabels, 0, labels, offset, partLabels.length)
              offset += partLabels.length
            }
            Option(labels)
          }
        case _ => None
      }
    }
  }

}
