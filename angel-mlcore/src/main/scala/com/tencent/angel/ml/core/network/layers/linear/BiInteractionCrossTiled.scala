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


package com.tencent.angel.ml.core.network.layers.linear

import com.tencent.angel.ml.core.conf.SharedConf
import com.tencent.angel.ml.core.network.Graph
import com.tencent.angel.ml.core.network.layers._
import com.tencent.angel.ml.core.utils.{LayerKeys, MLException}
import com.tencent.angel.ml.math2.matrix._
import com.tencent.angel.ml.math2.utils.VectorUtils
import com.tencent.angel.ml.math2.vector.{CompIntDoubleVector, CompIntFloatVector, IntDoubleVector, IntFloatVector, Vector}
import com.tencent.angel.ml.math2.{MFactory, VFactory}
import org.apache.commons.logging.LogFactory
import org.json4s.JsonDSL._
import org.json4s.JsonAST._

import scala.collection.mutable.ArrayBuffer


class BiInteractionCrossTiled(name: String, outputDim: Int, inputLayer: Layer)(implicit graph: Graph)
  extends LinearLayer(name, outputDim, inputLayer) {
  val LOG = LogFactory.getLog(classOf[BiInteractionCrossTiled])

  override protected def doForward(input: Matrix): Matrix = {
    val batchSize = graph.placeHolder.getBatchSize

    input match {
      case mat: RBCompIntDoubleMatrix =>
        val compMat = new Array[CompIntDoubleVector](batchSize)
        (0 until batchSize).foreach { row =>
          val partitions = mat.getRow(row).getPartitions
          val partitionLength = partitions.length
          val vecLength = (partitionLength * (partitionLength - 1) * 0.5).toInt
          val biInteractedRowPartitions = new Array[IntDoubleVector](vecLength)
          var outIndex = 0
          (0 until partitionLength - 1).foreach { i =>
            ((i+1) until partitionLength).foreach { j =>
              val singleCrossed = partitions(i).imul(partitions(j)).asInstanceOf[IntDoubleVector]
              biInteractedRowPartitions(outIndex) = singleCrossed
              outIndex += 1
            }
          }
          compMat(row) = VFactory.compIntDoubleVector(outputDim,biInteractedRowPartitions)
        }
        MFactory.rbCompIntDoubleMatrix(compMat)
      case mat: RBCompIntFloatMatrix =>
        val compMat = new Array[CompIntFloatVector](batchSize)
        (0 until batchSize).foreach { row =>
          val partitions = mat.getRow(row).getPartitions
          val partitionLength = partitions.length
          val vecLength = (partitionLength * (partitionLength - 1) * 0.5).toInt
          val biInteractedRowPartitions = new Array[IntFloatVector](vecLength)
          var outIndex = 0
          (0 until partitionLength - 1).foreach { i =>
            ((i+1) until partitionLength).foreach { j =>
              val singleCrossed = partitions(i).imul(partitions(j)).asInstanceOf[IntFloatVector]
              biInteractedRowPartitions(outIndex) = singleCrossed
              outIndex += 1
            }
          }
          compMat(row) = VFactory.compIntFloatVector(outputDim,biInteractedRowPartitions)
        }
        MFactory.rbCompIntFloatMatrix(compMat)
      case _ => throw MLException("ERROR! Only Comp Matrix is Supported!")
    }
  }

  override protected def doBackward(input: Matrix, gradInput: Matrix): Matrix = {
    val batchSize = graph.placeHolder.getBatchSize
    SharedConf.valueType() match {
      case "double" =>
        val inputData = input.asInstanceOf[RBCompIntDoubleMatrix]
        val gradInputData = gradInput.asInstanceOf[RBCompIntDoubleMatrix]

        val compMat = new Array[CompIntDoubleVector](batchSize)
        (0 until batchSize).foreach { row =>
          val partitionLength = inputData.getRow(row).getPartitions.length
          val vecLength = (partitionLength * (partitionLength - 1) * 0.5).toInt
          val index1 = ArrayBuffer[Int]()
          val index2 = ArrayBuffer[Int]()
          val i = ((partitionLength - 1) to 1 by -1).toArray
          val j = (0 to (partitionLength - 2)).toArray
          i.indices.foreach { index =>
            index1 ++= new Array[Int](i(index)).map(_+j(index))
            index2 ++= Array.range(j(index)+1, partitionLength)
          }
          val compMatRowPartitions = new Array[IntDoubleVector](partitionLength)
          (0 until partitionLength).foreach { p =>
            compMatRowPartitions(p) = VFactory.denseDoubleVector(inputData.getSubDim)
          }
          val inputRowPartitions = inputData.getRow(row).getPartitions
          val gradRowPartitions = gradInputData.getRow(row).getPartitions
          (0 until vecLength).foreach { crossedIndex =>
            val x = index1(crossedIndex)
            val y = index2(crossedIndex)
            compMatRowPartitions(x).iadd(inputRowPartitions(y).imul(gradRowPartitions(crossedIndex)))
            compMatRowPartitions(y).iadd(inputRowPartitions(x).imul(gradRowPartitions(crossedIndex)))
          }
          compMat(row) = VFactory.compIntDoubleVector(inputData.getRow(row).getDim, compMatRowPartitions)
        }
        MFactory.rbCompIntDoubleMatrix(compMat)
      case "float" =>
        val inputData = input.asInstanceOf[RBCompIntFloatMatrix]
        val gradInputData = gradInput.asInstanceOf[RBCompIntFloatMatrix]

        val compMat = new Array[CompIntFloatVector](batchSize)
        (0 until batchSize).foreach { row =>
          val partitionLength = inputData.getRow(row).getPartitions.length
          val vecLength = (partitionLength * (partitionLength - 1) * 0.5).toInt

          val index1 = ArrayBuffer[Int]()
          val index2 = ArrayBuffer[Int]()
          val i = ((partitionLength - 1) to 1 by -1).toArray
          val j = (0 to (partitionLength - 2)).toArray
          i.indices.foreach { index =>
            index1 ++= new Array[Int](i(index)).map(_+j(index))
            index2 ++= Array.range(j(index)+1, partitionLength)
          }
          val compMatRowPartitions = new Array[IntFloatVector](partitionLength)
          (0 until partitionLength).foreach { p =>
            compMatRowPartitions(p) = VFactory.denseFloatVector(inputData.getSubDim)
          }
          val inputRowPartitions = inputData.getRow(row).getPartitions
          val gradRowPartitions = gradInputData.getRow(row).getPartitions
          (0 until vecLength).foreach { crossedIndex =>
            val x = index1(crossedIndex)
            val y = index2(crossedIndex)
            compMatRowPartitions(x).iadd(inputRowPartitions(y).imul(gradRowPartitions(crossedIndex)))
            compMatRowPartitions(y).iadd(inputRowPartitions(x).imul(gradRowPartitions(crossedIndex)))
          }
          compMat(row) = VFactory.compIntFloatVector(inputData.getRow(row).getDim, compMatRowPartitions)
        }
        MFactory.rbCompIntFloatMatrix(compMat)
      case _ => throw MLException("Only Comp Matrix is Supported!")
    }
  }

  override def toString: String = {
    s"BiInteractionCross name=$name outputDim=$outputDim"
  }

  override def toJson: JField = {
    val layerJson = (LayerKeys.typeKey -> s"${this.getClass.getSimpleName}") ~
      (LayerKeys.outputDimKey -> outputDim) ~
      (LayerKeys.inputLayerKey, JString(inputLayer.name))

    JField(name, layerJson)
  }


}