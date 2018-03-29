/*
 * Tencent is pleased to support the open source community by making Angel available.
 *
 * Copyright (C) 2017 THL A29 Limited, a Tencent company. All rights reserved.
 *
 * Licensed under the BSD 3-Clause License (the "License"); you may not use this file except in
 * compliance with the License. You may obtain a copy of the License at
 *
 * https://opensource.org/licenses/BSD-3-Clause
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 *
 */

package com.tencent.angel.ml.treemodels.gbdt.fp.psf

import java.util

import com.tencent.angel.ml.matrix.psf.get.base.{GetResult, PartitionGetResult}
import io.netty.buffer.ByteBuf

class FeatureRowsGetResult(featureRows: util.Map[Int, (Array[Int], Array[Int])]) extends GetResult {
  def getFeatureRows = featureRows
}

class FeatureRowsPartitionGetResult[@specialized(Byte, Short, Int) T <: scala.AnyVal](_featureRows: util.Map[Int, (Array[Int], Array[T])],
                                                                                      _numBin: Int) extends PartitionGetResult {
  var featureRows: util.Map[Int, (Array[Int], Array[T])] = _featureRows
  var numBin: Int = _numBin

  def this() = this(null, -1)

  def getFeatureRows = featureRows

  override def serialize(buf: ByteBuf): Unit = {
    buf.writeInt(numBin)
    buf.writeInt(featureRows.size())
    val iter = featureRows.entrySet().iterator()
    while (iter.hasNext) {
      val entry = iter.next()
      val rowId = entry.getKey
      val (indices, bins) = entry.getValue
      buf.writeInt(rowId)
      val nnz = indices.length
      buf.writeInt(nnz)
      if (numBin <= 256) {
        val typeTBins = bins.asInstanceOf[Array[Byte]]
        for (i <- 0 until nnz) {
          buf.writeInt(indices(i))
          buf.writeByte(typeTBins(i))
        }
      }
      else if (numBin <= 65536) {
        val typeTBins = bins.asInstanceOf[Array[Short]]
        for (i <- 0 until nnz) {
          buf.writeInt(indices(i))
          buf.writeShort(typeTBins(i))
        }
      }
      else {
        val typeTBins = bins.asInstanceOf[Array[Int]]
        for (i <- 0 until nnz) {
          buf.writeInt(indices(i))
          buf.writeInt(typeTBins(i))
        }
      }
    }
  }

  override def deserialize(buf: ByteBuf): Unit = {
    numBin = buf.readInt()
    val nrows = buf.readInt()
    featureRows = new util.HashMap[Int, (Array[Int], Array[T])](nrows)
    for (i <- 0 until nrows) {
      val rowId = buf.readInt()
      val nnz = buf.readInt()
      val indices = new Array[Int](nnz)
      var bins: Array[T] = null
      if (numBin <= 256) {
        val typeTBins = new Array[Byte](nnz)
        for (j <- 0 until nnz) {
          indices(j) = buf.readInt()
          typeTBins(j) = buf.readByte()
        }
        bins = typeTBins.asInstanceOf[Array[T]]
      }
      else if (numBin <= 65536) {
        val typeTBins = new Array[Short](nnz)
        for (j <- 0 until nnz) {
          indices(j) = buf.readInt()
          typeTBins(j) = buf.readShort()
        }
        bins = typeTBins.asInstanceOf[Array[T]]
      }
      else {
        val typeTBins = new Array[Int](nnz)
        for (j <- 0 until nnz) {
          indices(j) = buf.readInt()
          typeTBins(j) = buf.readInt()
        }
        bins = typeTBins.asInstanceOf[Array[T]]
      }
      featureRows.put(rowId, (indices, bins))
    }
  }

  override def bufferLen(): Int = {
    var res = 8
    val bytesPerBins = if (numBin <= 256) 1 else if (numBin <= 65536) 2 else 4
    val iter = featureRows.entrySet().iterator()
    while (iter.hasNext) {
      val entry = iter.next()
      val nnz = entry.getValue._1.length
      res += 8 + (4 + bytesPerBins) * nnz
    }
    res
  }
}
