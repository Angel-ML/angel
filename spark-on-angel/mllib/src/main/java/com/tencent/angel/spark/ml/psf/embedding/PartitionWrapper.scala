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


package com.tencent.angel.spark.ml.psf.embedding

import com.tencent.angel.ps.storage.matrix.ServerPartition
import com.tencent.angel.ps.storage.vector.ServerIntFloatRow

class PartitionWrapper(val part: ServerPartition, val partDim: Int, val order: Int) {
  val nodePerRow: Int = {
    val startCol = part.getPartitionKey.getStartCol
    val endCol = part.getPartitionKey.getEndCol
    val _nodePerRow = ((endCol - startCol) / (order * partDim)).toInt
    assert(partDim * order * _nodePerRow == endCol - startCol)
    _nodePerRow
  }

  def dot(inputVectorId: Int, outputVectorId: Int): Float = {
    val (inRowId, inColId) = getVector(inputVectorId, isInputVector = true)
    val (outRowId, outColId) = getVector(outputVectorId, isInputVector = false)

    val inputRow = part.getRow(inRowId).asInstanceOf[ServerIntFloatRow].getValues
    val outputRow = part.getRow(outRowId).asInstanceOf[ServerIntFloatRow].getValues

    var dot = 0.0f
    for (i <- 0 until partDim) {
      dot += inputRow(inColId + i) * outputRow(outColId + i)
    }
    dot
  }

  def axpy(a: Float, nodeId: Int, isInputVec: Boolean, y: Array[Float]): Unit = {
    val (rowId, colId) = getVector(nodeId, isInputVec)
    val row = part.getRow(rowId).asInstanceOf[ServerIntFloatRow].getValues
    for (i <- 0 until partDim)
      y(i) += a * row(colId + i)
  }

  def addToServer(nodeId: Int, isInputVec: Boolean, delta: Array[Float]): Unit = {
    val (rowId, colId) = getVector(nodeId, isInputVec)
    val row = part.getRow(rowId).asInstanceOf[ServerIntFloatRow].getValues
    for (i <- 0 until partDim)
      row(colId + i) += delta(i)
  }

  def slice(from: Int, size: Int): Array[Float] = {
    val vec = Array.ofDim[Float](size * partDim)
    for (i <- 0 until size) {
      val (rowId, colId) = getVector(i + from, isInputVector = true)
      val row = part.getRow(rowId).asInstanceOf[ServerIntFloatRow].getValues
      System.arraycopy(row, colId, vec, i * partDim, partDim)
    }
    vec
  }

  private def getVector(nodeId: Int, isInputVector: Boolean): (Int, Int) = {
    val rowId = nodeId / nodePerRow
    val begin = if (isInputVector || order == 1) (nodeId % nodePerRow) * partDim * order else
      (nodeId % nodePerRow) + partDim * order + partDim
    (rowId, begin)
  }
}