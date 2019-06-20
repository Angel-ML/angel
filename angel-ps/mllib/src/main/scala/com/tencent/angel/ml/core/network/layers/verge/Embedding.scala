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


package com.tencent.angel.ml.core.network.layers.verge

import java.lang.{Long => JLong}
import java.util.concurrent.Future
import java.util.{HashMap => JHashMap, Map => JMap}

import com.tencent.angel.exception.AngelException
import com.tencent.angel.ml.core.conf.SharedConf
import com.tencent.angel.ml.core.network.layers._
import com.tencent.angel.ml.core.optimizer.{OptUtils, Optimizer}
import com.tencent.angel.ml.core.utils.paramsutils.ParamKeys
import com.tencent.angel.ml.core.utils.{NetUtils, PSMatrixUtils}
import com.tencent.angel.ml.math2.matrix._
import com.tencent.angel.ml.math2.storage._
import com.tencent.angel.ml.math2.vector._
import com.tencent.angel.ml.math2.{MFactory, VFactory}
import com.tencent.angel.ml.matrix.RowType
import com.tencent.angel.ml.matrix.psf.update.RandomNormal
import com.tencent.angel.ml.matrix.psf.update.base.VoidResult
import com.tencent.angel.ml.psf.columns._
import com.tencent.angel.model.{MatrixLoadContext, MatrixSaveContext, ModelLoadContext, ModelSaveContext}
import com.tencent.angel.ps.server.data.request.RandomNormalInitFunc
import com.tencent.angel.psagent.PSAgentContext
import org.apache.commons.logging.LogFactory
import org.json4s.JsonAST._
import org.json4s.JsonDSL._


class Embedding(name: String, outputDim: Int, val numFactors: Int, override val optimizer: Optimizer)(implicit graph: AngelGraph)
  extends InputLayer(name, outputDim)(graph) with Trainable {
  val LOG = LogFactory.getLog(classOf[Embedding])

  graph.addTrainable(this)

  val sharedConf: SharedConf = graph.conf

  val modelType: RowType = SharedConf.modelType
  val blockSize: Int = SharedConf.blockSize
  val mode = SharedConf.runningMode()

  private val numSlot: Int = OptUtils.getSlotNum(optimizer)
  private val indexRange: Long = SharedConf.indexRange
  private val psRows: Int = (numSlot + 1) * numFactors
  private val validIndexNum = SharedConf.modelSize

  private val embedMatCtx = PSMatrixUtils.createPSMatrixCtx(s"${name}_embedding", psRows, indexRange, modelType)
  embedMatCtx.setValidIndexNum(validIndexNum)
  graph.addMatrixCtx(embedMatCtx)
  lazy val matrixId: Int = PSMatrixUtils.getMatrixId(s"${name}_embedding")

  @transient var forward: Matrix = _
  @transient var backward: Matrix = _
  @transient var embeddings: JMap[JLong, Vector] = _

  override def calBackward(): Matrix = {
    val start = System.currentTimeMillis()
    status match {
      case STATUS.Forward =>
        backward = gatherGrad()
        status = STATUS.Backward
      case _ =>
        throw new AngelException("Status Error, Should call forward before when calling backward!")
    }

    val end = System.currentTimeMillis()
    //    println(s"Embedding($name) calBackward = ${end - start} ms")
    backward
  }

  override def pullParams(epoch: Int): Unit = {
    val start = System.currentTimeMillis()
    val rows = (0 until numFactors).toArray
    val indices = graph.placeHolder.getIndices

    val param = if (epoch == 0) {
      val initFunc = new RandomNormalInitFunc(0.0, 0.00001)
      new GetColsParam(matrixId, rows, indices, initFunc)
    } else {
      new GetColsParam(matrixId, rows, indices)
    }

    val func = new GetColsFunc(param)
    val result = PSAgentContext.get.getUserRequestAdapter.get(func).get().asInstanceOf[GetColsResult]
    embeddings = result.results
    val end = System.currentTimeMillis()
  }

  def mergeUpdate(map: JMap[JLong, Vector], key: Long, update: Vector, value: Double): Unit = {
    if (!map.containsKey(key)) {
      if (value == 1) map.put(key, update)
      else map.put(key, update.imul(value))
    } else {
      if (value == 1) map.get(key).iadd(update)
      else map.get(key).iadd(update.imul(value))
    }
  }

  override def pushGradient(): Unit = {
    val start = System.currentTimeMillis()
    status match {
      case STATUS.Backward =>
        val map: JMap[JLong, Vector] = new JHashMap()
        backward match {
          case gradient: RBCompIntDoubleMatrix =>
            val rows = gradient.getRows
            assert(rows.length == graph.placeHolder.getBatchSize)
            val batchData: Matrix = graph.placeHolder.getFeats
            val batchSize = graph.placeHolder.getBatchSize
            (0 until batchSize).foreach { idx =>
              batchData.getRow(idx).getStorage match {
                case s: IntDoubleSortedVectorStorage =>
                  val values = s.getValues
                  val index = s.getIndices
                  var i = 0
                  while (i < index.length) {
                    val key = index(i)
                    val value = values(i)
                    val update = rows(idx).getPartitions()(i)
                    mergeUpdate(map, key.toLong, update, value)
                    i += 1
                  }
                case s: LongDoubleSortedVectorStorage =>
                  val values = s.getValues
                  val index = s.getIndices
                  var i = 0
                  while (i < index.length) {
                    val key = index(i)
                    val value = values(i)
                    val update = rows(idx).getPartitions()(i)
                    mergeUpdate(map, key, update, value)
                    i += 1
                  }
                case s: IntDoubleSparseVectorStorage =>
                  var i = 0
                  val indices = s.getIndices.sorted
                  while (i < indices.length) {
                    val key = indices(i)
                    val value = s.get(key)
                    val update = rows(idx).getPartitions()(i)
                    mergeUpdate(map, key.toLong, update, value)
                    i += 1
                  }
                case s: LongDoubleSparseVectorStorage =>
                  var i = 0
                  val indices = s.getIndices.sorted
                  while (i < indices.length) {
                    val key = indices(i)
                    val value = s.get(key)
                    val update = rows(idx).getPartitions()(i)
                    mergeUpdate(map, key, update, value)
                    i += 1
                  }
              }
            }

          case gradient: RBCompIntFloatMatrix =>
            val rows = gradient.getRows
            assert(rows.length == graph.placeHolder.getBatchSize)
            val batchData: Matrix = graph.placeHolder.getFeats
            val batchSize = graph.placeHolder.getBatchSize

            (0 until batchSize).foreach { idx =>
              batchData.getRow(idx).getStorage match {
                case s: IntFloatSortedVectorStorage =>
                  val values = s.getValues
                  val index = s.getIndices
                  var i = 0
                  while (i < index.length) {
                    val key = index(i)
                    val value = values(i)
                    val update = rows(idx).getPartitions()(i)
                    mergeUpdate(map, key.toLong, update, value)
                    i += 1
                  }
                case s: LongFloatSortedVectorStorage =>
                  val values = s.getValues
                  val index = s.getIndices
                  var i = 0
                  while (i < index.length) {
                    val key = index(i)
                    val value = values(i)
                    val update = rows(idx).getPartitions()(i)
                    mergeUpdate(map, key, update, value)
                    i += 1
                  }
                case s: IntFloatSparseVectorStorage =>
                  var i = 0
                  val indices = s.getIndices.sorted
                  while (i < indices.length) {
                    val key = indices(i)
                    val value = s.get(key)
                    val update = rows(idx).getPartitions()(i)
                    mergeUpdate(map, key.toLong, update, value)
                    i += 1
                  }
                case s: LongFloatSparseVectorStorage =>
                  var i = 0
                  val indices = s.getIndices.sorted
                  while (i < indices.length) {
                    val key = indices(i)
                    val value = s.get(key)
                    val update = rows(idx).getPartitions()(i)
                    mergeUpdate(map, key, update, value)
                    i += 1
                  }
              }
            }

          case _ =>
        }

        // Divide Gradient with TaskNum*BatchSize
        val divider = OptUtils.getNormal(mode, graph)
        val iter = map.values().iterator()
        while (iter.hasNext) {
          val vector = iter.next()
          vector.idiv(divider)
        }

        // Push Gradient
        val rowNums = (numFactors * numSlot until numFactors * (numSlot + 1)).toArray

        val param = new UpdateColsParam(matrixId, rowNums, graph.placeHolder.getIndices, map)
        val func = new UpdateColsFunc(param)
        PSAgentContext.get().getUserRequestAdapter.update(func).get()
        status = STATUS.Gradient
      case _ =>
    }
    val end = System.currentTimeMillis()
  }

  override def update(epoch: Int, batchSize: Int): Future[VoidResult] = {
    var result: Future[VoidResult] = null
    status match {
      case STATUS.Gradient =>
        result = optimizer.update(matrixId, numFactors, epoch, batchSize)
        status = STATUS.Update
      case _ => print("STATUS Error, please calculate Gradient first!")
    }
    result
  }

  override def calOutput(): Matrix = {
    val start = System.currentTimeMillis()
    status match {
      case STATUS.Null =>
        //        println(s"the status in Embedding($name)-calOutput is ${status.toString}")
        val batchSize = graph.placeHolder.getBatchSize
        val batchData: Matrix = graph.placeHolder.getFeats
        val compRows = (0 until batchSize).toArray.map { idx =>
          batchData.getRow(idx).getStorage match {
            case s: IntFloatSparseVectorStorage =>
              val index = s.getIndices.sorted
              VFactory.compIntFloatVector(
                if (outputDim <= 0) outputDim else index.length * numFactors,
                index.map { key =>
                  val value = s.get(key)
                  val emVector = embeddings.get(key.toLong)
                  if (value == 1) {
                    emVector.asInstanceOf[IntFloatVector]
                  } else {
                    emVector.mul(value).asInstanceOf[IntFloatVector]
                  }

                })
            case s: IntFloatSortedVectorStorage =>
              val index = s.getIndices
              val values = s.getValues
              val vectors = new Array[IntFloatVector](index.length)
              var i = 0
              while (i < index.length) {
                val emVector = embeddings.get(index(i).toLong)
                if (values(i) == 1)
                  vectors(i) = emVector.asInstanceOf[IntFloatVector]
                else
                  vectors(i) = emVector.mul(values(i)).asInstanceOf[IntFloatVector]
                i += 1
              }
              VFactory.compIntFloatVector(
                if (outputDim <= 0) outputDim else index.length * numFactors,
                vectors
              )
            case s: LongFloatSparseVectorStorage =>
              val index = s.getIndices.sorted
              VFactory.compIntFloatVector(
                if (outputDim <= 0) outputDim else index.length * numFactors,
                index.map { key =>
                  val value = s.get(key)
                  val emVector = embeddings.get(key)
                  if (value == 1) {
                    emVector.asInstanceOf[IntFloatVector]
                  } else {
                    emVector.mul(value).asInstanceOf[IntFloatVector]
                  }
                })
            case s: IntDoubleSparseVectorStorage =>
              val index = s.getIndices.sorted
              VFactory.compIntDoubleVector(
                if (outputDim <= 0) outputDim else index.length * numFactors,
                index.map { key =>
                  val value = s.get(key)
                  val emVector = embeddings.get(key.toLong)
                  if (value == 1) {
                    emVector.asInstanceOf[IntDoubleVector]
                  } else {
                    emVector.mul(value).asInstanceOf[IntDoubleVector]
                  }
                })
            case s: LongDoubleSparseVectorStorage =>
              val index = s.getIndices.sorted
              VFactory.compIntDoubleVector(
                if (outputDim <= 0) outputDim else index.length * numFactors,
                index.map { key =>
                  val value = s.get(key)
                  val emVector = embeddings.get(key)
                  if (value == 1) {
                    emVector.asInstanceOf[IntDoubleVector]
                  } else {
                    emVector.mul(value).asInstanceOf[IntDoubleVector]
                  }
                })
            case _ => throw new AngelException("Only float and double are supported!")
          }
        }

        forward = compRows.head match {
          case _: CompIntFloatVector =>
            MFactory.rbCompIntFloatMatrix(compRows.map {
              _.asInstanceOf[CompIntFloatVector]
            })
          case _: CompIntDoubleVector =>
            MFactory.rbCompIntDoubleMatrix(compRows.map {
              _.asInstanceOf[CompIntDoubleVector]
            })
        }

        status = STATUS.Forward
      case _ =>
    }

    val end = System.currentTimeMillis()
    //    println(s"Embedding($name) calOutput ${end - start} ms")
    forward
  }

  override def init(taskflag: Int): Unit = {
    val bound: Double = 0.00001
    NetUtils.storageType(modelType) match {
      case "dense" | "component_dense" =>
        if (taskflag == 0) {
          val randFunc = new RandomNormal(matrixId, 0, numFactors, 0.0, bound)
          PSAgentContext.get().getUserRequestAdapter.update(randFunc).get()
        }
      case _ =>
    }
  }

  override def toString: String = {
    s"Embedding name=$name outputDim=$outputDim optimizer=$optimizer"
  }

  override def loadParams(loadContext: ModelLoadContext): Unit = {
    loadContext.addMatrix(new MatrixLoadContext(embedMatCtx.getName))
  }

  override def saveParams(saveContext: ModelSaveContext): Unit = {
    val outputFormat = SharedConf.embeddingLayerMatrixOutputFormat
    val embedMatMCS: MatrixSaveContext = new MatrixSaveContext(embedMatCtx.getName, outputFormat)
    embedMatMCS.addIndices((0 until numFactors).toArray)
    saveContext.addMatrix(embedMatMCS)
  }

  override def toJson: JObject = {
    (ParamKeys.name -> name) ~
      (ParamKeys.typeName -> s"${this.getClass.getSimpleName}") ~
      (ParamKeys.outputDim -> outputDim) ~
      (ParamKeys.numFactors -> numFactors) ~
      (ParamKeys.optimizer -> optimizer.toJson)
  }
}
