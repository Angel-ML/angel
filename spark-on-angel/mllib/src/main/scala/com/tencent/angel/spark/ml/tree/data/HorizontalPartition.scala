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


package com.tencent.angel.spark.ml.tree.data

import com.tencent.angel.spark.ml.tree.gbdt.metadata.FeatureInfo
import com.tencent.angel.spark.ml.tree.sketch.HeapQuantileSketch
import com.tencent.angel.spark.ml.tree.util.{EvenPartitioner, IdenticalPartitioner, Maths}
import org.apache.spark.TaskContext
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD

object HorizontalPartition {

  def getLabels(partitions: RDD[HorizontalPartition]): Array[Float] = {
    val partLabels = partitions.mapPartitionsWithIndex((partId, iterator) => {
      if (iterator.hasNext)
        Iterator((partId, iterator.next().labels))
      else
        Iterator.empty
    }).collect().sortBy(_._1)
    val numInstance = partLabels.map(_._2.length).sum
    val labels = new Array[Float](numInstance)
    var offset = 0
    partLabels.foreach {
      case (id, pl) => {
        Array.copy(pl, 0, labels, offset, pl.length)
        offset += pl.length
      }
    }
    labels
  }

  def getCandidateSplits(partitions: RDD[HorizontalPartition], numFeature: Int,
                         numSplit: Int, numPartition: Int): Seq[(Int, Array[Float])] = {
    val featEdges = new EvenPartitioner(numFeature, numPartition).partitionEdges()

    partitions.mapPartitions(iterator => {
      val hps = iterator.toArray
      require(hps.length == numPartition)
      hps.map(hp => {
        val partId = hp.partId
        val featLo = featEdges(partId)
        val featHi = featEdges(partId + 1)
        val sketches = new Array[HeapQuantileSketch](featHi - featLo)
        val indices = hp.indices
        val values = hp.values
        for (i <- indices.indices) {
          val fid = indices(i)
          if (sketches(fid - featLo) == null)
            sketches(fid - featLo) = new HeapQuantileSketch()
          sketches(fid - featLo).update(values(i))
        }
        (partId, sketches.map(sketch => Option(sketch)))
      }).iterator
    }, preservesPartitioning = true)
      .partitionBy(new IdenticalPartitioner(numPartition))
      .mapPartitions(iterator => {
        val (partIds, localSketches) = iterator.toArray.unzip
        require(partIds.distinct.length == 1)
        val partId = partIds(0)
        val featLo = featEdges(partId)
        val featHi = featEdges(partId + 1)
        require(localSketches.forall(_.length == featHi - featLo))
        val splits = new Array[Array[Float]](featHi - featLo)
        for (i <- splits.indices) {
          var globalSketch = null.asInstanceOf[HeapQuantileSketch]
          localSketches.foreach(localSketch => {
            if (localSketch(i).isDefined) {
              if (globalSketch == null) {
                globalSketch = localSketch(i).get
              } else {
                globalSketch.merge(localSketch(i).get)
              }
            }
          })
          if (globalSketch != null) {
            splits(i) = Maths.unique(globalSketch.getQuantiles(numSplit))
          }
        }
        splits.view.zipWithIndex.map {
          case (s, i) => (i + featLo, s)
        }.filter(_._2 == null).iterator
      }, preservesPartitioning = true)
      .collect()
  }

  def toVPDataset(partitions: RDD[HorizontalPartition],
                  numInstance: Int, numFeature: Int, numPartition: Int,
                  bcFeatureInfo: Broadcast[FeatureInfo]): RDD[DataSet] = {
    val featEdges = new EvenPartitioner(numFeature, numPartition).partitionEdges()

    partitions.map(hp => {
      val partId = hp.partId
      val partSize = hp.indexEnd.length
      val fids = new Array[Short](hp.indices.length)
      val bins = new Array[Byte](hp.indexEnd.length)
      val featLo = featEdges(partId)
      for (i <- fids.indices) {
        fids(i) = (hp.indices(i) - featLo + Short.MinValue).toShort
        bins(i) = (Maths.indexOf(bcFeatureInfo.value.getSplits(hp.indices(i)),
          hp.values(i)) + Byte.MinValue).toByte
      }
      (partId, (TaskContext.getPartitionId(), fids, bins, hp.indexEnd))
    }).partitionBy(new IdenticalPartitioner(numPartition))
      .mapPartitionsWithIndex((partId, iterator) => {
        val parts = iterator.toArray.sortBy(_._2._1)
        val dataset = new DataSet(parts.length, numInstance)
        val featLo = featEdges(partId)
        parts.foreach {
          case (id, (originPartId, fids, bins, indexEnd)) =>
            require(id == partId)
            val indices = fids.map(x => x.toInt - Short.MinValue + featLo)
            val values = bins.map(x => x.toInt - Byte.MinValue)
            dataset.setPartition(originPartId, indices, values, indexEnd)
            println(s"OriPart[$originPartId] has ${fids.length} instances, $numInstance in total")
        }
        Iterator(dataset)
      })
  }
}

case class HorizontalPartition(partId: Int, labels: Array[Float], indexEnd: Array[Int],
                               indices: Array[Int], values: Array[Float])

