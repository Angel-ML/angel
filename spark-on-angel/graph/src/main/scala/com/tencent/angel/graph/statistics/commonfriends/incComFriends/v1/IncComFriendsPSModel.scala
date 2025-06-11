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
package com.tencent.angel.graph.statistics.commonfriends.incComFriends.v1

import com.tencent.angel.graph.common.param.ModelContext
import com.tencent.angel.graph.common.psf.param.{LongKeysGetParam, LongKeysUpdateParam}
import com.tencent.angel.graph.common.psf.result.GetLongsResult
import com.tencent.angel.graph.model.neighbor.dynamic.psf.getbyte.GetByteNbrs
import com.tencent.angel.graph.model.neighbor.dynamic.psf.initbyte.Sort.SortParam
import com.tencent.angel.graph.model.neighbor.dynamic.psf.initbyte.{InitByteNbrs, Sort}
import com.tencent.angel.graph.utils.ModelContextUtils
import com.tencent.angel.ml.math2.VFactory
import com.tencent.angel.ml.math2.vector.LongIntVector
import com.tencent.angel.ml.matrix.RowType
import com.tencent.angel.model.output.format.SnapshotFormat
import com.tencent.angel.model.{MatrixSaveContext, ModelSaveContext}
import com.tencent.angel.ps.storage.vector.element.{ByteArrayElement, IElement}
import com.tencent.angel.spark.context.PSContext
import com.tencent.angel.spark.models.PSMatrix
import com.twitter.chill.ScalaKryoInstantiator
import it.unimi.dsi.fastutil.longs.Long2ObjectOpenHashMap
import org.apache.spark.rdd.RDD

import scala.collection.mutable.ArrayBuffer

class IncComFriendsPSModel(nbrModelContext: ModelContext,
                           tagModelContext: ModelContext) extends Serializable {

  var neighborMatrix: PSMatrix = _
  var tagMatrix: PSMatrix = _

  def init(): Unit = {
    val nbrMc = ModelContextUtils.createMatrixContext(
      nbrModelContext, RowType.T_ANY_LONGKEY_SPARSE, classOf[ByteArrayElement])
    nbrMc.setParts(nbrMc.getParts)
    neighborMatrix = PSMatrix.matrix(nbrMc)

    val tagMc = ModelContextUtils.createMatrixContext(tagModelContext, RowType.T_INT_SPARSE_LONGKEY)
    tagMc.setParts(tagMc.getParts)
    tagMatrix = PSMatrix.matrix(tagMc)
  }

  def updateTag(nodes: RDD[Long]): Double = {
    nodes.mapPartitions { iter =>
      Iterator.single(updateTag(iter.toArray))
    }.sum()
  }

  def updateTag(nodes: Array[Long]): Int = {
    val len = nodes.length
    val msgs = VFactory.sparseLongKeyIntVector(len, nodes, Array.fill(len)(1))
    tagMatrix.update(msgs)
    println(s"init ${nodes.length} taggedNodes.")
    nodes.length
  }

  def readTag(nodes: Array[Long]): LongIntVector = {
    tagMatrix.asyncPull(0, nodes).get().asInstanceOf[LongIntVector]
  }

  def initNbrTable(data: RDD[(Long, Long)], batchSize: Int): Unit = {
    data.mapPartitions { iter => {
      iter.sliding(batchSize, batchSize).map(pairs => initNbrTable(neighborMatrix, pairs))
    }
    }.count()
  }

  def initNbrTable(psMatrix: PSMatrix, pairs: Seq[(Long, Long)]): Long = {
    var startTime = System.currentTimeMillis()
    val aggreResult = new Long2ObjectOpenHashMap[ArrayBuffer[Long]]()
    pairs.foreach { case (src, dst) =>
      var neighbors: ArrayBuffer[Long] = aggreResult.get(src)
      if (neighbors == null) {
        neighbors = new ArrayBuffer[Long]()
        neighbors.append(dst)
        aggreResult.put(src, neighbors)
      } else
        neighbors.append(dst)
    }

    val nodeIds = aggreResult.keySet().toLongArray()
    val neighbors = nodeIds.map(x => new ByteArrayElement(ScalaKryoInstantiator.defaultPool.toBytesWithoutClass(aggreResult.get(x).toArray)).asInstanceOf[IElement])
    aggreResult.clear()
    val aggreTime = System.currentTimeMillis() - startTime

    startTime = System.currentTimeMillis()
    val func = new InitByteNbrs(new LongKeysUpdateParam(psMatrix.id, nodeIds, neighbors))
    psMatrix.asyncPsfUpdate(func).get()
    val pushTime = System.currentTimeMillis() - startTime
    println(s"incInit ${pairs.length} edges (${nodeIds.length} nodes with neighbors), processTime=$aggreTime, pushTime=$pushTime, ")
    aggreResult.clear()
    pairs.length.toLong
  }

  def initIncNbrTable(data: RDD[(Long, Array[Long])], batchSize: Int): Unit = {
    data.mapPartitions { iter => {
      iter.sliding(batchSize, batchSize).map(pairs => initIncNbrTable(neighborMatrix, pairs.toArray))
    }
    }.count()
  }

  def initIncNbrTable(psMatrix: PSMatrix, pairs: Array[(Long, Array[Long])]): Int = {
    var startTime = System.currentTimeMillis()
    val nodeIds = new Array[Long](pairs.length)
    val neighbors = new Array[IElement](pairs.length)

    for (i <- pairs.indices) {
      nodeIds(i) = pairs(i)._1
      neighbors(i) = new ByteArrayElement(ScalaKryoInstantiator.defaultPool.toBytesWithoutClass(pairs(i)._2)).asInstanceOf[IElement]
    }
    val processTime = System.currentTimeMillis() - startTime
    startTime = System.currentTimeMillis()
    val func = new InitByteNbrs(new LongKeysUpdateParam(psMatrix.id, nodeIds, neighbors))
    psMatrix.asyncPsfUpdate(func).get()
    val pushTime = System.currentTimeMillis() - startTime
    println(s"incInit ${pairs.length} nodes with neighbors), processTime=$processTime, pushTime=$pushTime, ")
    pairs.length
  }

  def getNbrTable(nodeIds: Array[Long], count: Int = -1): Long2ObjectOpenHashMap[Array[Long]] = {
    neighborMatrix.psfGet(new GetByteNbrs(
      new LongKeysGetParam(neighborMatrix.id, nodeIds))).asInstanceOf[GetLongsResult].getData
  }

  def trans(): Unit = {
    val func = new Sort(new SortParam(neighborMatrix.id, false))
    neighborMatrix.asyncPsfUpdate(func).get()
  }

  def checkpointTag(): Unit = {
    tagMatrix.checkpoint()
  }

  def checkpoint(epochId: Int=0) = {
    val modelSaveContext = new ModelSaveContext()
    modelSaveContext.addMatrix(new MatrixSaveContext(tagMatrix.name, classOf[SnapshotFormat].getTypeName))
    modelSaveContext.addMatrix(new MatrixSaveContext(neighborMatrix.name, classOf[SnapshotFormat].getTypeName))
    PSContext.instance().checkpoint(epochId, modelSaveContext)
  }

}
