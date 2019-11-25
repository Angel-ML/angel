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

package com.tencent.angel.spark.ml.graph.node2vec

import java.util

import com.tencent.angel.graph.client.node2vec.getfuncs.getdegreebucket.{PullDegreeBucket, PullDegreeBucketParam, PullDegreeBucketResult}
import com.tencent.angel.graph.client.node2vec.getfuncs.getmaxdegree.{PullMaxDegree, PullMaxDegreeResult}
import com.tencent.angel.graph.client.node2vec.getfuncs.getmindegree.{PullMinDegree, PullMinDegreeResult}
import com.tencent.angel.graph.client.node2vec.getfuncs.pullneighbor.{PullNeighbor, PullNeighborParam, PullNeighborResult}
import com.tencent.angel.graph.client.sampleneighbor.{SampleNeighbor, SampleNeighborParam, SampleNeighborResult}
import com.tencent.angel.ml.matrix.psf.get.base.GetParam
import com.tencent.angel.spark.context.PSContext
import com.tencent.angel.spark.ml.graph.data.NeighborTablePartition
import com.tencent.angel.spark.models.PSMatrix
import it.unimi.dsi.fastutil.longs.Long2ObjectOpenHashMap
import org.apache.spark.rdd.RDD

import scala.collection.mutable.ArrayBuffer
import scala.reflect.ClassTag

trait NeighborUtils extends Serializable {
  protected val neighbor: PSMatrix

  protected def getDegreeThreshold(numBuckets: Int = 100, hr: Double = 0.5): Int = {
    val getMin = new PullMinDegree(new GetParam(neighbor.id))
    val minVal = neighbor.psfGet(getMin).asInstanceOf[PullMinDegreeResult].getResult
    // println(minVal)

    val getMax = new PullMaxDegree(new GetParam(neighbor.id))
    val maxVal = neighbor.psfGet(getMax).asInstanceOf[PullMaxDegreeResult].getResult
    // println(maxVal)

    val getBucket = new PullDegreeBucket(new PullDegreeBucketParam(neighbor.id, maxVal, minVal, numBuckets))
    val bucket = neighbor.psfGet(getBucket).asInstanceOf[PullDegreeBucketResult].getResult

    val keys = bucket.keySet().toIntArray
    util.Arrays.sort(keys)
    val binLen = (maxVal - minVal + 1.0) / numBuckets

    var (avgX, avgY, avgXY, avgX2) = (0.0, 0.0, 0.0, 0.0)
    val numEles = keys.size
    keys.foreach { key =>
      // get x and y of power law distribution buy using interpolation
      val x = (Math.floor(key * binLen) + Math.floor((key + 1) * binLen)) / 2.0
      val y = 1.0 * bucket.get(key) / binLen

      // y = C x^{-\alpha} -> \log{y} = \log{C} - \alpha \log{x}
      // so transform x and y to log(x) and log(y), so that log(x) is linear to log(y)
      val logX = Math.log(x)
      val logY = Math.log(y)

      // println(s"$x, $y, $logX, $logY")

      // to calculate \alpha, it's required avgX, avgY, avgXY, avgX2
      avgX += logX / numEles
      avgY += logY / numEles
      avgXY += logX * logY / numEles
      avgX2 += logX * logX / numEles
    }

    // calculate the alpha of power law distribution
    val alpha = -(avgXY - avgX * avgY) / (avgX2 - avgX * avgX)

    // HR = 1 - \frac{t^{1-\alpha} - 1}{n^{2-\alpha} - 1}
    // -> t^{2-\alpha} = 1 + (1 - HR)(n^{2-\alpha} - 1)
    // -> t = \exp{(\frac{1 + (1 - HR)(n^{2-\alpha} - 1)}{2-\alpha})}
    val k = 2 - alpha
    val a = 1 + (1 - hr) * (Math.pow(maxVal, k) - 1)
    val threshold = Math.exp(Math.log(a) / k).toInt

    threshold
  }

  protected def pullNeighborTable[@specialized(Int, Long) ED: ClassTag](nodeIds: Array[ED], count: Int = -1): AnyRef = {
    val tag = implicitly[ClassTag[ED]]
    tag.runtimeClass match {
      case clz if clz == classOf[Int] =>
        neighbor.psfGet(new SampleNeighbor(
          new SampleNeighborParam(neighbor.id, nodeIds.asInstanceOf[Array[Int]], count))
        ).asInstanceOf[SampleNeighborResult].getNodeIdToNeighbors
      case clz if clz == classOf[Long] =>
        neighbor.psfGet(new PullNeighbor(
          new PullNeighborParam(neighbor.id, nodeIds.asInstanceOf[Array[Long]]))
        ).asInstanceOf[PullNeighborResult].getResult
    }
  }
}

class AliasUtils(override protected val neighbor: PSMatrix) extends NeighborUtils with Serializable {
  type AliasTableKey = (Long, Long)
  type AliasTableValue = (Array[Long], Array[Long], Array[Long])

  def createAliasTable(neighborTable: RDD[NeighborTablePartition[Long]],
                       degreeBinSize: Int, hitRatio: Double): RDD[Map[(Long, Long), (Array[Long], Array[Long], Array[Long])]] = {
    val numPartition = neighborTable.getNumPartitions
    val threshold: Int = getDegreeThreshold(degreeBinSize, hitRatio)
    val aliasUnShuffle = neighborTable.mapPartitions { partIter =>
      PSContext.instance()
      val part = partIter.next()
      val srcIds = part.srcIds
      val neighbors: Array[Set[Long]] = part.neighbors.map(_.toSet)
      val srcMap = srcIds.zipWithIndex.toMap
      val numEdge = neighbors.map(_.size).sum

      val batchSize = 1024
      val dstBuf = new ArrayBuffer[Long](batchSize)
      val keyBuf = new ArrayBuffer[AliasTableKey](batchSize)
      val aliasTable = new ArrayBuffer[(AliasTableKey, AliasTableValue)](numEdge)
      val cache = new Long2ObjectOpenHashMap[Set[Long]]()

      srcIds.zip(part.neighbors).foreach { case (src, neighbor: Array[Long]) =>
        neighbor.foreach {
          case neigh if src < neigh =>
            if (dstBuf.size == batchSize || keyBuf.size == batchSize) {
              createBatchAliasTable(srcMap, neighbors, cache, threshold, aliasTable, dstBuf, keyBuf)
            }

            if (!srcMap.contains(neigh) && !cache.containsKey(neigh)) {
              dstBuf.append(neigh)
            }
            keyBuf.append(src -> neigh)
          case _ =>
        }
      }

      if (keyBuf.nonEmpty) {
        createBatchAliasTable(srcMap, neighbors, cache, threshold, aliasTable, dstBuf, keyBuf)
      }

      cache.clear()
      aliasTable.iterator
    }

    val aliasShuffle = aliasUnShuffle.partitionBy(new AngelHashPartitioner(numPartition)).mapPartitions { iter =>
      Iterator.single(iter.toMap)
    }
    aliasShuffle.count()
    aliasShuffle
  }

  private def createBatchAliasTable(srcMap: Map[Long, Int], neighbors: Array[Set[Long]],
                                    cache: Long2ObjectOpenHashMap[Set[Long]], threshold: Int,
                                    aliasTable: ArrayBuffer[(AliasTableKey, AliasTableValue)],
                                    dstBuf: ArrayBuffer[Long], keyBuf: ArrayBuffer[AliasTableKey]): Unit = {
    val pulledNeighborTable = if (dstBuf.nonEmpty) {
      pullNeighborTable[Long](dstBuf.toArray)
        .asInstanceOf[Long2ObjectOpenHashMap[Array[Long]]]
    } else {
      null.asInstanceOf[Long2ObjectOpenHashMap[Array[Long]]]
    }

    keyBuf.foreach { case (s, d) =>
      val sn = neighbors(srcMap(s))
      val dn = if (srcMap.contains(d)) {
        neighbors(srcMap(d))
      } else {
        if (cache.containsKey(d)) {
          cache.get(d)
        } else {
          val distNeigh = pulledNeighborTable.get(d).toSet
          if (distNeigh.size >= threshold) {
            cache.put(d, distNeigh)
          }
          distNeigh
        }
      }

      val com = sn & dn
      val srcSide = sn -- com
      val dstSide = dn -- com

      aliasTable.append((s, d) -> (srcSide.toArray, com.toArray, dstSide.toArray))
    }

    dstBuf.clear()
    keyBuf.clear()
  }
}
