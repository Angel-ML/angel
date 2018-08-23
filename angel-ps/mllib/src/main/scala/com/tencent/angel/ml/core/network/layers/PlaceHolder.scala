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


package com.tencent.angel.ml.core.network.layers

import com.tencent.angel.exception.AngelException
import com.tencent.angel.ml.core.conf.SharedConf
import com.tencent.angel.ml.feature.LabeledData
import com.tencent.angel.ml.math2.matrix._
import com.tencent.angel.ml.math2.storage._
import com.tencent.angel.ml.math2.vector._
import com.tencent.angel.ml.math2.{MFactory, VFactory}
import it.unimi.dsi.fastutil.ints.IntOpenHashSet
import it.unimi.dsi.fastutil.longs.LongOpenHashSet
import org.apache.commons.logging.LogFactory

import scala.util.Sorting.quickSort


class PlaceHolder(val conf: SharedConf) extends Serializable {
  val LOG = LogFactory.getLog(classOf[PlaceHolder])

  def this() = this(SharedConf.get())

  var data: Array[LabeledData] = _
  var feats: Matrix = _
  var labels: Matrix = _
  var indices: Vector = _

  var isFeed: Boolean = false

  def feedData(data: Array[LabeledData]): Unit = {
    feats = null
    labels = null
    indices = null

    this.data = data
  }

  def isDense: Boolean = {
    SharedConf.inputDataFormat match {
      case "dummy" | "libsvm" => false
      case "dense" => true
    }
  }

  def getFeats: Matrix = {
    val batchSize = data.length

    if (feats == null) {
      feats = data.head.getX match {
        case v: IntDoubleVector =>
          if (v.isDense) {
            val matTemp = MFactory.denseDoubleMatrix(batchSize, v.getDim)
            data.zipWithIndex.map { case (ld: LabeledData, row: Int) =>
              matTemp.setRow(row, ld.getX.asInstanceOf[IntDoubleVector])
            }
            matTemp
          } else {
            MFactory.rbIntDoubleMatrix(data.map(_.getX.asInstanceOf[IntDoubleVector]))
          }
        case v: IntFloatVector =>
          if (v.isDense) {
            val matTemp = MFactory.denseFloatMatrix(batchSize, v.getDim)
            data.zipWithIndex.map { case (ld: LabeledData, row: Int) =>
              matTemp.setRow(row, ld.getX.asInstanceOf[IntFloatVector])
            }
            matTemp
          } else {
            MFactory.rbIntFloatMatrix(data.map(_.getX.asInstanceOf[IntFloatVector]))
          }
        case _: LongDoubleVector =>
          MFactory.rbLongDoubleMatrix(data.map(_.getX.asInstanceOf[LongDoubleVector]))
        case _: LongFloatVector =>
          MFactory.rbLongFloatMatrix(data.map(_.getX.asInstanceOf[LongFloatVector]))
        case _ => throw new AngelException("RowType is not support!")
      }
    }

    feats
  }

  def getLabel: Matrix = {
    labels = if (labels == null) {
      MFactory.denseFloatMatrix(data.length, 1, data.map(_.getY.toFloat))
    } else {
      labels
    }

    labels
  }

  def getBatchSize: Int = data.length

  def getFeatDim: Long = {
    data.head.getX match {
      case v: IntKeyVector => v.getDim
      case v: LongKeyVector => v.getDim
    }
  }

  def getIndices: Vector = synchronized {
    //    LOG.error(s"indices is null = ${indices == null}")
    if (indices == null) {
      SharedConf.keyType() match {
        case "int" =>
          val temSet = new IntOpenHashSet()
          data.foreach(ld =>
            ld.getX.getStorage match {
              case s: IntKeyVectorStorage if !s.isDense =>
                s.getIndices.foreach(i => temSet.add(i))
              case s: IntKeyVectorStorage if s.isDense =>
                (0 until ld.getX.asInstanceOf[IntKeyVector].getDim).foreach(i => temSet.add(i))
              case _ =>
            }
          )

          val colIndex = temSet.toIntArray
          //          LOG.error(s"PlaceHolder ${colIndex.mkString(" ")}")
          quickSort(colIndex)
          //          LOG.error(s"PlaceHolder ${colIndex.mkString(" ")}")

          indices = VFactory.denseIntVector(colIndex)
        case "long" =>
          val temSet = new LongOpenHashSet()
          data.foreach(ld =>
            ld.getX.getStorage match {
              case s: LongKeyVectorStorage if !s.isDense =>
                s.getIndices.foreach(i => temSet.add(i))
              case s: LongKeyVectorStorage if s.isDense =>
                throw new AngelException("Long Dense Vector is not supported!")
              case _ =>
            }
          )
          val colIndex = temSet.toLongArray
          quickSort(colIndex)
          indices = VFactory.denseLongVector(colIndex)
        case _ =>
          throw new AngelException("key type should be int or long")
      }
    }

    indices
  }
}
