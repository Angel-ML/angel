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

import io.netty.buffer.ByteBuf
import org.apache.commons.logging.{Log, LogFactory}

import com.tencent.angel.PartitionKey
import com.tencent.angel.ml.matrix.psf.get.base._
import com.tencent.angel.psagent.PSAgentContext
import com.tencent.angel.spark.ml.psf.embedding.NESlice.{SliceParam, SlicePartitionParam, SlicePartitionResult, SliceResult}


class NESlice(sliceParam: SliceParam) extends GetFunc(sliceParam) {
  val log: Log = LogFactory.getLog(this.getClass)

  def this(matrixId: Int, from: Int, until: Int, parDim: Int, order: Int) =
    this(new SliceParam(matrixId: Int, from: Int, until: Int, parDim: Int, order: Int))

  def this() = this(null)

  override def partitionGet(pParam: PartitionGetParam): PartitionGetResult = {
    val part = psContext.getMatrixStorageManager.getPart(pParam.getMatrixId, pParam.getPartKey.getPartitionId)
    if (part == null) return null
    val partParam = pParam.asInstanceOf[SlicePartitionParam]
    val pw = new PartitionWrapper(part, partParam.getPartDim, partParam.getOrder)
    val from = partParam.getFrom
    val size = partParam.getSize
    val fArray = pw.slice(from, size)
    val index = (pParam.getPartKey.getStartCol / (pParam.getPartKey.getEndCol - pParam.getPartKey.getStartCol)).toInt
    new SlicePartitionResult(fArray, index)
  }

  override def merge(partResults: java.util.List[PartitionGetResult]): GetResult = {
    val numPart = partResults.size()
    assert(numPart > 0)
    val result = new Array[Array[Float]](numPart)
    val partIter = partResults.iterator()
    while (partIter.hasNext) {
      val part = partIter.next().asInstanceOf[SlicePartitionResult]
      result(part.getPartIndex) = part.getResult
    }
    new SliceResult(result, sliceParam.from, sliceParam.size, sliceParam.partDim)
  }
}

object NESlice {

  class SliceParam(matrixId: Int,
                   var from: Int,
                   var size: Int,
                   var partDim: Int,
                   var order: Int) extends GetParam(matrixId) {
    def this() = this(-1, 0, 0, 0, 0)

    override def split(): java.util.List[PartitionGetParam] = {
      val parts = PSAgentContext.get().getMatrixMetaManager.getPartitions(matrixId)
      val partNum = parts.size()
      val partParams = new java.util.ArrayList[PartitionGetParam](partNum)
      for (i <- 0 until partNum) {
        partParams.add(new SlicePartitionParam(matrixId, parts.get(i), from, size, partDim, order))
      }
      partParams
    }
  }

  class SlicePartitionParam(matrixId: Int,
                            partKey: PartitionKey,
                            var from: Int,
                            var size: Int,
                            var partDim: Int,
                            var order: Int)
    extends PartitionGetParam(matrixId, partKey) {
    def this() = this(-1, null, 0, 0, 0, 0)

    def getFrom: Int = from

    def getSize: Int = size

    def getPartDim: Int = partDim

    def getOrder: Int = order

    override def serialize(buf: ByteBuf) {
      super.serialize(buf)
      buf.writeInt(from)
      buf.writeInt(size)
      buf.writeInt(partDim)
      buf.writeInt(order)
    }

    override def deserialize(buf: ByteBuf) {
      super.deserialize(buf)
      this.from = buf.readInt()
      this.size = buf.readInt()
      this.partDim = buf.readInt()
      this.order = buf.readInt()
    }

    override def bufferLen(): Int = super.bufferLen() + 16
  }

  /**
    * `FullPartitionAggrResult` is a result Class for all Aggregate function whose result is double[][]
    */
  class SlicePartitionResult(private var result: Array[Float], private var partIndex: Int) extends PartitionGetResult {
    def this() = this(null, 0)

    def getResult: Array[Float] = result

    def getPartIndex: Int = partIndex

    override def serialize(buf: ByteBuf) {
      buf.writeInt(result.length)
      for (i <- 0 until result.length) buf.writeFloat(result(i))
      buf.writeInt(partIndex)
    }

    override def deserialize(buf: ByteBuf) {
      val size = buf.readInt()
      result = new Array[Float](size)
      for (i <- result.indices) result(i) = buf.readFloat()
      this.result = result
      this.partIndex = buf.readInt()
    }

    override def bufferLen(): Int = 12 + result.length * 4
  }

  class SliceResult(val partSlices: Array[Array[Float]], from: Int, size: Int, partDim: Int) extends GetResult {
    def getSlice(sep: String = " "): Array[String] = {
      Array.tabulate(size) { i =>
        partSlices.map(_.slice(i * partDim, (i + 1) * partDim).mkString(sep)).mkString(s"${from + i}:", sep, "")
      }
    }
  }

}
