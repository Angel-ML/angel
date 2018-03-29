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

import java.nio.ByteBuffer
import java.util

import com.tencent.angel.PartitionKey
import com.tencent.angel.exception.AngelException
import com.tencent.angel.ml.matrix.psf.update.enhance.{PartitionUpdateParam, UpdateFunc, UpdateParam}
import com.tencent.angel.ps.impl.PSContext
import com.tencent.angel.ps.impl.matrix.ServerDenseFloatRow
import com.tencent.angel.psagent.PSAgentContext
import io.netty.buffer.ByteBuf
import org.apache.commons.logging.{Log, LogFactory}

class FeatureRowsUpdateFunc[@specialized(Byte, Short, Int) T <: scala.AnyVal](param: FeatureRowsUpdateParam[T]) extends UpdateFunc(param) {
  val LOG: Log = LogFactory.getLog(classOf[FeatureRowsUpdateFunc[T]])

  def this() = this(null)

  override def partitionUpdate(partParam: PartitionUpdateParam): Unit = {
    val part = psContext.getMatrixStorageManager
      .getPart(partParam.getMatrixId(), partParam.getPartKey().getPartitionId())

    if (part != null) {
      val param = partParam.asInstanceOf[FeatureRowsPartitionUpdateParam[T]]
      val startRow = part.getPartitionKey.getStartRow
      val endRow = part.getPartitionKey.getEndRow
      val iter = param.featureRows.entrySet().iterator()
      while (iter.hasNext) {
        val entry = iter.next()
        val rowId = entry.getKey
        val (indices, bins) = entry.getValue
        if (startRow <= rowId && endRow > rowId) {
          val row = part.getRow(rowId)
          if (row == null) {
            throw new AngelException(s"Get null row: $rowId")
          }
          /*if (row.getRowType == T_FLOAT_DENSE)
            setFeatureRow(row.asInstanceOf[ServerDenseFloatRow],
              indices, bins, param)*/
          setFeatureRow(row.asInstanceOf[ServerDenseFloatRow],
            indices, bins, param)
        }
      }
    }
  }

  private def setFeatureRow(row: ServerDenseFloatRow, indices: Array[Int], bins: Array[T],
                            param: FeatureRowsPartitionUpdateParam[T]) {
    try {
      row.getLock.writeLock().lock()
      val buf: ByteBuffer = ByteBuffer.wrap(row.getDataArray)
      var numSet: Int = buf.getInt(0)
      if (numSet == param.numWorker) numSet = 0
      //LOG.info(s"Row[${row.getRowId}] Before[$numSet]")

      var pos: Int = 4
      val bytesPerBin = if (param.numBin <= 256) 1
      else if (param.numBin <= 65536) 2 else 4
      var skip = numSet
      while (skip > 0) {
        //val workerId = buf.getInt(pos)
        val nnzOfOneWorker = buf.getInt(pos + 4)
        //LOG.info(s"Row[${row.getRowId}] $numSet-th update: Worker[$workerId] has $nnzOfOneWorker nnz")
        pos += 8 + (4 + bytesPerBin) * nnzOfOneWorker
        skip -= 1
      }

      buf.putInt(pos, param.workerId);
      pos += 4
      buf.putInt(pos, indices.length);
      pos += 4
      for (indice <- indices) {
        buf.putInt(pos, indice); pos += 4
      }
      if (bytesPerBin == 1) {
        val typeTBins = bins.asInstanceOf[Array[Byte]]
        for (bin <- typeTBins) {
          buf.put(pos, bin); pos += 1
        }
      }
      else if (bytesPerBin == 2) {
        val typeTBins = bins.asInstanceOf[Array[Short]]
        for (bin <- typeTBins) {
          buf.putShort(pos, bin); pos += 2
        }
      }
      else {
        val typeTBins = bins.asInstanceOf[Array[Int]]
        for (bin <- typeTBins) {
          buf.putInt(pos, bin); pos += 4
        }
      }

      buf.putInt(0, numSet + 1)
    } finally {
      row.getLock.writeLock().unlock()
    }
  }
}

class FeatureRowsUpdateParam[@specialized(Byte, Short, Int) T <: scala.AnyVal](matrixId: Int, updateClock: Boolean,
                                                                               numWorker: Int, workerId: Int,
                                                                               nrows: Int, numBin: Int) extends UpdateParam(matrixId, updateClock) {
  val LOG: Log = LogFactory.getLog(classOf[FeatureRowsUpdateParam[T]])

  val rowIndexes: util.List[Int] = new util.ArrayList[Int](nrows)
  val rowIndices: util.List[Array[Int]] = new util.ArrayList[Array[Int]](nrows)
  val rowBins: util.List[Array[T]] = new util.ArrayList[Array[T]](nrows)

  def set(rowId: Int, indices: Array[Int], bins: Array[Int]): Unit = {
    rowIndexes.add(rowId)
    rowIndices.add(indices)

    if (numBin <= 256) {
      val typeTBins = new Array[Byte](bins.length)
      for (i <- bins.indices)
        typeTBins(i) = (bins(i) + Byte.MinValue).toByte
      rowBins.add(typeTBins.asInstanceOf[Array[T]])
    }
    else if (numBin <= 65536) {
      val typeTBins = new Array[Short](bins.length)
      for (i <- bins.indices)
        typeTBins(i) = (bins(i) + Short.MinValue).toShort
      rowBins.add(typeTBins.asInstanceOf[Array[T]])
    }
    else {
      val typeTBins = new Array[Int](bins.length)
      for (i <- bins.indices)
        typeTBins(i) = (bins(i) + Int.MinValue).toInt
      rowBins.add(typeTBins.asInstanceOf[Array[T]])
    }

    /*var typeTBins: Array[T] = null

    var minValue: Int = 0
    if (numBin <= 256) {
      minValue = Byte.MinValue
      typeTBins = (new Array[Byte](bins.length)).asInstanceOf[Array[T]]
    }
    else if (numBin <= 65536) {
      minValue = Short.MinValue
      typeTBins = (new Array[Short](bins.length)).asInstanceOf[Array[T]]
    }
    else {
      minValue = Int.MinValue
      typeTBins = (new Array[Int](bins.length)).asInstanceOf[Array[T]]
    }
    for (i <- bins.indices)
      typeTBins(i) = (bins(i) + minValue).asInstanceOf[T]
    rowBins.add(typeTBins)*/
  }

  override def split(): util.List[PartitionUpdateParam] = {
    val partList: util.List[PartitionKey] = PSAgentContext.get
      .getMatrixMetaManager.getPartitions(getMatrixId)
    val size: Int = partList.size

    val partParamList: util.List[PartitionUpdateParam] =
      new util.ArrayList[PartitionUpdateParam](size)
    for (i <- 0 until partList.size()) {
      val partKey: PartitionKey = partList.get(i)
      val featureRows: util.Map[Int, (Array[Int], Array[T])] =
        new util.HashMap[Int, (Array[Int], Array[T])](nrows)
      //LOG.info(s"Part start[${partKey.getStartRow}], end[${partKey.getEndRow}]")
      for (j <- 0 until rowIndexes.size()) {
        val rowId: Int = rowIndexes.get(j)
        //LOG.info(s"row[$rowId], start[${partKey.getStartRow}], end[${partKey.getEndRow}]")
        if (partKey.getStartRow <= rowId && partKey.getEndRow > rowId) {
          featureRows.put(rowId, (rowIndices.get(j), rowBins.get(j)))
        }
      }

      if (featureRows.size() > 0) {
        //LOG.info(s"Part[$i] has ${featureRows.size()} rows")
        partParamList.add(new FeatureRowsPartitionUpdateParam[T](super.getMatrixId,
          partKey, super.isUpdateClock, featureRows, numWorker, workerId, numBin))
      }
    }
    partParamList
  }
}

class FeatureRowsPartitionUpdateParam[@specialized(Byte, Short, Int) T <: scala.AnyVal](matrixId: Int, partKey: PartitionKey, updateClock: Boolean,
                                                                                        _featureRows: util.Map[Int, (Array[Int], Array[T])],
                                                                                        _numWorker: Int, _workerId: Int, _numBin: Int) extends PartitionUpdateParam(matrixId, partKey, updateClock) {
  var featureRows: util.Map[Int, (Array[Int], Array[T])] = _featureRows
  var numWorker: Int = _numWorker
  var workerId: Int = _workerId
  var numBin: Int = _numBin

  def this() = this(0, null, false, null, -1, -1, -1)

  override def serialize(buf: ByteBuf): Unit = {
    super.serialize(buf)
    buf.writeInt(numWorker)
    buf.writeInt(workerId)
    buf.writeInt(numBin)
    buf.writeInt(featureRows.size())
    if (numBin <= 256) {
      val iter = featureRows.entrySet().iterator()
      while (iter.hasNext) {
        val entry = iter.next()
        buf.writeInt(entry.getKey)
        val (indices, bins) = entry.getValue.asInstanceOf[(Array[Int], Array[Byte])]
        buf.writeInt(indices.length)
        for (i <- indices.indices) {
          buf.writeInt(indices(i))
          buf.writeByte(bins(i))
        }
      }
    }
    else if (numBin <= 65536) {
      val iter = featureRows.entrySet().iterator()
      while (iter.hasNext) {
        val entry = iter.next()
        buf.writeInt(entry.getKey)
        val (indices, bins) = entry.getValue.asInstanceOf[(Array[Int], Array[Short])]
        buf.writeInt(indices.length)
        for (i <- indices.indices) {
          buf.writeInt(indices(i))
          buf.writeShort(bins(i))
        }
      }
    }
    else {
      val iter = featureRows.entrySet().iterator()
      while (iter.hasNext) {
        val entry = iter.next()
        buf.writeInt(entry.getKey)
        val (indices, bins) = entry.getValue.asInstanceOf[(Array[Int], Array[Int])]
        buf.writeInt(indices.length)
        for (i <- indices.indices) {
          buf.writeInt(indices(i))
          buf.writeInt(bins(i))
        }
      }
    }
  }

  override def deserialize(buf: ByteBuf): Unit = {
    super.deserialize(buf)
    numWorker = buf.readInt()
    workerId = buf.readInt()
    numBin = buf.readInt()
    val nrows: Int = buf.readInt()
    featureRows = new util.HashMap[Int, (Array[Int], Array[T])]()
    if (numBin <= 256) {
      for (i <- 0 until nrows) {
        val rowId = buf.readInt()
        val nnz = buf.readInt()
        val indices = new Array[Int](nnz)
        val bins = new Array[Byte](nnz)
        for (j <- 0 until nnz) {
          indices(j) = buf.readInt()
          bins(j) = buf.readByte()
        }
        featureRows.put(rowId, (indices, bins.asInstanceOf[Array[T]]))
      }
    }
    else if (numBin <= 65536) {
      for (i <- 0 until nrows) {
        val rowId = buf.readInt()
        val nnz = buf.readInt()
        val indices = new Array[Int](nnz)
        val bins = new Array[Short](nnz)
        for (j <- 0 until nnz) {
          indices(j) = buf.readInt()
          bins(j) = buf.readShort()
        }
        featureRows.put(rowId, (indices, bins.asInstanceOf[Array[T]]))
      }
    }
    else {
      for (i <- 0 until nrows) {
        val rowId = buf.readInt()
        val nnz = buf.readInt()
        val indices = new Array[Int](nnz)
        val bins = new Array[Int](nnz)
        for (j <- 0 until nnz) {
          indices(j) = buf.readInt()
          bins(j) = buf.readInt()
        }
        featureRows.put(rowId, (indices, bins.asInstanceOf[Array[T]]))
      }
    }
  }

  override def bufferLen(): Int = {
    var res = super.bufferLen() + 16
    val bytesPerBin = if (numBin <= 256) 1 else if (numBin <= 65536) 2 else 4
    val iter = featureRows.entrySet().iterator()
    while (iter.hasNext) {
      val entry = iter.next()
      res += 4 + entry.getValue._1.length * (4 + bytesPerBin)
    }
    res
  }
}
