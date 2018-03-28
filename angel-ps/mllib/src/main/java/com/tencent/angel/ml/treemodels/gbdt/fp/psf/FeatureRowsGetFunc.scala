package com.tencent.angel.ml.treemodels.gbdt.fp.psf

import java.nio.ByteBuffer
import java.util

import com.tencent.angel.PartitionKey
import com.tencent.angel.ml.matrix.psf.get.base._
import com.tencent.angel.ps.impl.PSContext
import com.tencent.angel.ps.impl.matrix.ServerDenseFloatRow
import com.tencent.angel.psagent.PSAgentContext
import io.netty.buffer.ByteBuf
import org.apache.commons.logging.{Log, LogFactory}

class FeatureRowsGetFunc[@specialized(Byte, Short, Int) T <: scala.AnyVal](param: FeatureRowsGetParam[T]) extends GetFunc(param) {
  val LOG: Log = LogFactory.getLog(classOf[FeatureRowsGetFunc[T]])

  def this() = this(null)

  def this(matrixId: Int, numWorker: Int, rowIndexes: Array[Int], numBin: Int) =
    this(new FeatureRowsGetParam[T](matrixId, numWorker, rowIndexes, numBin))

  override def partitionGet(partParam: PartitionGetParam): PartitionGetResult = {
    val param = partParam.asInstanceOf[FeatureRowsPartitionGetParam[T]]
    val rowIndexes = param.getRowIndexes
    val numWorker = param.getNumWorker
    val numBin = param.getNumBin

    val nrows = rowIndexes.size()
    val bytesPerBin = if (numBin <= 256) 1 else if (numBin <= 65536) 2 else 4
    val featureRows: util.Map[Int, (Array[Int], Array[T])] =
      new util.HashMap[Int, (Array[Int], Array[T])](nrows)

    for (i <- 0 until nrows) {
      val rowId = rowIndexes.get(i)
      // ToDo
      //val row: ServerDenseFloatRow = psContext.getMatrixStorageManager.getRow(
      //  param.getMatrixId, rowId, param.getPartKey.getPartitionId).asInstanceOf[ServerDenseFloatRow]

      val row: ServerDenseFloatRow = null

      val buf: ByteBuffer = ByteBuffer.wrap(row.getDataArray)
      val numSet = buf.getInt(0)
      if (numSet == numWorker) {
        //LOG.info(s"Row[${row.getRowId}] Set $numSet")
        var rowNnz = 0
        val offsets = new Array[Int](numSet)
        var pos = 4
        for (j <- 0 until numWorker) {
          val workerId = buf.getInt(pos)
          val nnzOfOneWorker = buf.getInt(pos + 4)
          //LOG.info(s"Row[${row.getRowId}] Worker[$workerId] has $nnzOfOneWorker nnz")
          rowNnz += nnzOfOneWorker
          for (k <- (workerId + 1) until numWorker)
            offsets(k) += nnzOfOneWorker
          pos += 8 + (4 + bytesPerBin) * nnzOfOneWorker
        }

        val rowIndices = new Array[Int](rowNnz)
        var rowBins: Array[T] = null
        pos = 4
        for (j <- 0 until numWorker) {
          val workerId = buf.getInt(pos); pos += 4
          val nnzOfOneWorker = buf.getInt(pos); pos += 4
          val offset = offsets(workerId)
          for (k <- 0 until nnzOfOneWorker) {
            rowIndices(offset + k) = buf.getInt(pos); pos += 4
          }
          if (bytesPerBin == 1) {
            if (rowBins == null)
              rowBins = new Array[Byte](rowNnz).asInstanceOf[Array[T]]
            val typeTRowBins = rowBins.asInstanceOf[Array[Byte]]
            for (k <- 0 until nnzOfOneWorker) {
              typeTRowBins(offset + k) = buf.get(pos); pos += 1
            }
          }
          else if (bytesPerBin == 2) {
            if (rowBins == null)
              rowBins = new Array[Short](rowNnz).asInstanceOf[Array[T]]
            val typeTRowBins = rowBins.asInstanceOf[Array[Short]]
            for (k <- 0 until nnzOfOneWorker) {
              typeTRowBins(offset + k) = buf.getShort(pos); pos += 2
            }
          }
          else {
            if (rowBins == null)
              rowBins = new Array[Int](rowNnz).asInstanceOf[Array[T]]
            val typeTRowBins = rowBins.asInstanceOf[Array[Int]]
            for (k <- 0 until nnzOfOneWorker) {
              typeTRowBins(offset + k) = buf.getInt(pos); pos += 4
            }
          }
        }
        featureRows.put(rowId, (rowIndices, rowBins))
      }
    }
    new FeatureRowsPartitionGetResult[T](featureRows, numBin)
  }

  override def merge(partResults: util.List[PartitionGetResult]): GetResult = {
    val results = partResults.asInstanceOf[util.List[FeatureRowsPartitionGetResult[T]]]

    var nrows: Int = 0
    for (i <- 0 until results.size()) {
      nrows += results.get(i).getFeatureRows.size()
    }

    val featureRows: util.Map[Int, (Array[Int], Array[Int])] =
      new util.HashMap[Int, (Array[Int], Array[Int])](nrows)

    val getParam = super.getParam.asInstanceOf[FeatureRowsGetParam[T]]
    val numBin = getParam.getNumBin

    for (i <- 0 until results.size()) {
      val partFeatRows = results.get(i).getFeatureRows
      //featureRows.putAll(partFeatRows)
      val iter = partFeatRows.entrySet().iterator()
      while (iter.hasNext) {
        val entry = iter.next()
        val rowId = entry.getKey
        val (indices, bins) = entry.getValue
        val nnz = indices.length
        val typeIntBins = new Array[Int](nnz)

        if (numBin <= 256) {
          val typeTBins = bins.asInstanceOf[Array[Byte]]
          for (j <- 0 until nnz) {
            typeIntBins(j) = typeTBins(j).toInt - Byte.MinValue
          }
        }
        else if (numBin <= 65536) {
          val typeTBins = bins.asInstanceOf[Array[Short]]
          for (j <- 0 until nnz) {
            typeIntBins(j) = typeTBins(j).toInt - Short.MinValue
          }
        }
        else {
          val typeTBins = bins.asInstanceOf[Array[Int]]
          for (j <- 0 until nnz) {
            typeIntBins(j) = typeTBins(j) - Int.MinValue
          }
        }
        featureRows.put(rowId, (indices, typeIntBins))
      }
    }

    new FeatureRowsGetResult(featureRows)
  }
}

class FeatureRowsGetParam[@specialized(Byte, Short, Int) T <: scala.AnyVal](matrixId: Int, numWorker: Int,
                          rowIndexes: Array[Int], numBin: Int) extends GetParam(matrixId) {
  def getNumWorker = numWorker

  def getRowIndexes = rowIndexes

  def getNumBin = numBin

  override def split(): util.List[PartitionGetParam] = {
    val partList: util.List[PartitionKey] = PSAgentContext.get
      .getMatrixMetaManager.getPartitions(super.getMatrixId)
    val size: Int = partList.size

    val partParams: util.List[PartitionGetParam] = new util.ArrayList[PartitionGetParam]
    for (i <- 0 until size) {
      val partKey = partList.get(i)
      val partRowIndexes: util.List[Int] = new util.ArrayList[Int](rowIndexes.length)
      for (rowId <- rowIndexes) {
        if (partKey.getStartRow <= rowId && partKey.getEndRow > rowId) {
          partRowIndexes.add(rowId)
        }
      }

      if (partRowIndexes.size() > 0) {
        partParams.add(new FeatureRowsPartitionGetParam[T](
          super.getMatrixId, partKey, partRowIndexes, numWorker, numBin))
      }
    }

    partParams
  }
}

class FeatureRowsPartitionGetParam[@specialized(Byte, Short, Int) T <: scala.AnyVal](matrixId: Int, partKey: PartitionKey, _rowIndexes: util.List[Int],
                                                      _numWorker: Int, _numBin: Int) extends PartitionGetParam(matrixId, partKey) {
  var rowIndexes: util.List[Int] = _rowIndexes
  var numWorker: Int = _numWorker
  var numBin: Int = _numBin

  def this() = this(-1, null, null, -1, -1)

  def getRowIndexes = rowIndexes

  def getNumWorker = numWorker

  def getNumBin = numBin

  override def serialize(buf: ByteBuf): Unit = {
    super.serialize(buf)
    val nrows: Int = rowIndexes.size()
    buf.writeInt(nrows)
    for (i <- 0 until nrows)
      buf.writeInt(rowIndexes.get(i))
    buf.writeInt(numWorker)
    buf.writeInt(numBin)
  }

  override def deserialize(buf: ByteBuf): Unit = {
    super.deserialize(buf)
    val nrows: Int = buf.readInt()
    rowIndexes = new util.ArrayList[Int](nrows)
    for (i <- 0 until nrows)
      rowIndexes.add(buf.readInt())
    numWorker = buf.readInt()
    numBin = buf.readInt()
  }

  override def bufferLen(): Int = {
    super.bufferLen() + 12 + 4 * rowIndexes.size()
  }
}
