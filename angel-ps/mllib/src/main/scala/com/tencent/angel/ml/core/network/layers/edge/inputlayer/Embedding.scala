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


package com.tencent.angel.ml.core.network.layers.edge.inputlayer

import java.lang.{Long => JLong}
import java.util.{HashMap => JHashMap, Map => JMap}

import com.tencent.angel.client.AngelClient
import com.tencent.angel.conf.{AngelConf, MatrixConf}
import com.tencent.angel.exception.AngelException
import com.tencent.angel.ml.core.conf.SharedConf
import com.tencent.angel.ml.core.network.layers._
import com.tencent.angel.ml.core.optimizer.{OptUtils, Optimizer}
import com.tencent.angel.ml.core.utils.PSMatrixUtils
import com.tencent.angel.ml.math2.matrix._
import com.tencent.angel.ml.math2.storage._
import com.tencent.angel.ml.math2.vector._
import com.tencent.angel.ml.math2.{MFactory, VFactory}
import com.tencent.angel.ml.matrix.RowType
import com.tencent.angel.ml.matrix.psf.update.RandomNormal
import com.tencent.angel.ml.psf.columns._
import com.tencent.angel.model.{MatrixSaveContext, ModelSaveContext}
import com.tencent.angel.psagent.PSAgentContext
import org.apache.commons.logging.LogFactory

import scala.util.Sorting.quickSort

class Embedding(name: String, outputDim: Int, val numFactors: Int, override val optimizer: Optimizer)(implicit graph: AngelGraph)
  extends InputLayer(name, outputDim)(graph) with Trainable {
  val LOG = LogFactory.getLog(classOf[Embedding])

  graph.addTrainable(this)

  val sharedConf: SharedConf = graph.conf

  val modelType: RowType = SharedConf.denseModelType
  val blockSize: Int = SharedConf.blockSize

  private val multiplier: Int = OptUtils.getOptMultiplier(optimizer)
  private val indexRange: Int = SharedConf.indexRange.toInt
  private val psRows: Int = multiplier * numFactors

  private val embedMatCtx = PSMatrixUtils.createPSMatrixCtx(s"${name}_embedding", psRows, indexRange, modelType)
  graph.addMatrixCtx(embedMatCtx)
  lazy val matrixId: Int = PSMatrixUtils.getMatrixId(s"${name}_embedding")

  @transient var forward: Matrix = _
  @transient var backward: Matrix = _
  @transient var embeddings: JMap[JLong, Vector] = _

  override def calBackward(): Matrix = {
    val start = System.currentTimeMillis()
    status match {
      case STATUS.Forward =>
        //        println(s"the status in Embedding($name)-calBackward is ${status.toString}")
        backward = gatherGrad()
        status = STATUS.Backward
      case _ =>
        throw new AngelException("Status Error, Should call forward before when calling backward!")
    }

    val end = System.currentTimeMillis()
    //    println(s"Embedding($name) calBackward = ${end - start} ms")
    backward
  }

  override def pullParams(): Unit = {
    val start = System.currentTimeMillis()
    val rows = (0 until numFactors).toArray
    val indices = graph.placeHolder.getIndices
    val param = new GetColsParam(matrixId, rows, indices)
    val func = new GetColsFunc(param)
    val result = PSAgentContext.get.getUserRequestAdapter.get(func).asInstanceOf[GetColsResult]
    embeddings = result.results
    val end = System.currentTimeMillis()
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
            (0 until batchSize).map { idx =>
              batchData.getRow(idx) match {
                case v: IntDoubleVector =>
                  v.getStorage.getIndices.zip(rows(idx).getPartitions).map { case (f, update) =>
                    val value = v.get(f)
                    if (!map.containsKey(f.toLong)) {
                      if (value == 1) {
                        map.put(f.toLong, update)
                      } else {
                        map.put(f.toLong, update.imul(value))
                      }
                    } else {
                      if (value == 1) {
                        map.get(f.toLong).iadd(update)
                      } else {
                        map.get(f.toLong).iadd(update.imul(v.get(f)))
                      }
                    }
                  }

                case v: LongDoubleVector =>
                  v.getStorage.getIndices.zip(rows(idx).getPartitions).map { case (f, update) =>
                    val value = v.get(f)
                    if (!map.containsKey(f)) {
                      if (value == 1) {
                        map.put(f, update)
                      } else {
                        map.put(f, update.imul(value))
                      }
                    } else {
                      if (value == 1) {
                        map.get(f).iadd(update)
                      } else {
                        map.get(f).iadd(update.imul(v.get(f)))
                      }
                    }
                  }
              }
            }

          case gradient: RBCompIntFloatMatrix =>
            val rows = gradient.getRows
            assert(rows.length == graph.placeHolder.getBatchSize)
            val batchData: Matrix = graph.placeHolder.getFeats
            val batchSize = graph.placeHolder.getBatchSize
            (0 until batchSize).map { idx =>
              batchData.getRow(idx) match {
                case v: IntFloatVector =>
                  v.getStorage.getIndices.zip(rows(idx).getPartitions).map { case (f, update) =>
                    val value = v.get(f)
                    if (!map.containsKey(f.toLong)) {
                      if (value == 1) {
                        map.put(f.toLong, update)
                      } else {
                        map.put(f.toLong, update.imul(value))
                      }
                    } else {
                      if (value == 1) {
                        map.get(f.toLong).iadd(update)
                      } else {
                        map.get(f.toLong).iadd(update.imul(v.get(f)))
                      }
                    }
                  }

                case v: LongFloatVector =>
                  v.getStorage.getIndices.zip(rows(idx).getPartitions).map { case (f, update) =>
                    val value = v.get(f)
                    if (!map.containsKey(f)) {
                      if (value == 1) {
                        map.put(f, update)
                      } else {
                        map.put(f, update.imul(value))
                      }
                    } else {
                      if (value == 1) {
                        map.get(f).iadd(update)
                      } else {
                        map.get(f).iadd(update.imul(v.get(f)))
                      }
                    }
                  }
              }
            }

          case _ =>
        }

        // Divide Gradient with TaskNum*BatchSize
        val divider = graph.taskNum * graph.placeHolder.getBatchSize
        val iter = map.values().iterator()
        while (iter.hasNext) {
          val vector = iter.next()
          vector.idiv(divider)
        }

        // Push Gradient
        val rowNums = (numFactors * (multiplier - 1) until numFactors * multiplier).toArray

        val param = new UpdateColsParam(matrixId, rowNums, graph.placeHolder.getIndices, map)
        val func = new UpdateColsFunc(param)
        PSAgentContext.get().getUserRequestAdapter.update(func).get()
        status = STATUS.Gradient
      case _ =>
    }
    val end = System.currentTimeMillis()
    //    LOG.error(s"Embedding push = ${end - start} ms")
  }

  override def update(epoch: Int): Unit = {
    status match {
      case STATUS.Gradient =>
        optimizer.update(matrixId, numFactors, epoch)
        status = STATUS.Update
      case _ => print("STATUS Error, please calculate Gradient first!")
    }
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
              val index = s.getIndices
              quickSort(index)
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
            case s: LongFloatSparseVectorStorage =>
              val index = s.getIndices
              quickSort(index)
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
              val index = s.getIndices
              quickSort(index)
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
              val index = s.getIndices
              quickSort(index)
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

  override def init(taskflag: Int, initIndexVector: Vector = null): Unit = {
    val bound: Double = 0.00001
    if (taskflag == 0) {
      val randFunc = new RandomNormal(matrixId, 0, numFactors, 0.0, bound)
      PSAgentContext.get().getUserRequestAdapter.update(randFunc)
    }
  }

  override def toString: String = {
    s"Embedding name=$name outputDim=$outputDim optimizer=$optimizer"
  }

  override def loadParams(client: AngelClient): Unit = {
    SharedConf.actionType().toLowerCase match {
      case "train" =>
        embedMatCtx.set(MatrixConf.MATRIX_SAVE_PATH, sharedConf.get(AngelConf.ANGEL_SAVE_MODEL_PATH))
      case "inctrain" =>
        embedMatCtx.set(MatrixConf.MATRIX_SAVE_PATH, sharedConf.get(AngelConf.ANGEL_SAVE_MODEL_PATH))
        embedMatCtx.set(MatrixConf.MATRIX_LOAD_PATH, sharedConf.get(AngelConf.ANGEL_LOAD_MODEL_PATH))

        embedMatCtx.init(client.getConf)
      case "predict" =>
        embedMatCtx.set(MatrixConf.MATRIX_LOAD_PATH, sharedConf.get(AngelConf.ANGEL_LOAD_MODEL_PATH))

        embedMatCtx.init(client.getConf)
    }

    client.addMatrix(embedMatCtx)
  }

  override def saveParams(saveContext: ModelSaveContext): Unit = {
    SharedConf.actionType().toLowerCase match {
      case "train" | "inctrain" =>
        val embedMatMCS: MatrixSaveContext = new MatrixSaveContext(embedMatCtx.getName)
        embedMatMCS.addIndices((0 until numFactors).toArray)
        saveContext.addMatrix(embedMatMCS)
      case _ =>
    }
  }
}
