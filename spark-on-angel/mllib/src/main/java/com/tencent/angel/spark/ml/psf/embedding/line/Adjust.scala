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


package com.tencent.angel.spark.ml.psf.embedding.line

import java.util.Random
import scala.collection.JavaConversions._

import io.netty.buffer.ByteBuf
import org.apache.commons.logging.{Log, LogFactory}

import com.tencent.angel.PartitionKey
import com.tencent.angel.ml.matrix.psf.update.base.{PartitionUpdateParam, UpdateFunc, UpdateParam}
import com.tencent.angel.psagent.PSAgentContext
import com.tencent.angel.spark.ml.psf.embedding.line.Adjust.{AdjustParam, AdjustPartitionParam}
import com.tencent.angel.spark.ml.psf.embedding.{NENegativeSample, PartitionWrapper}

class Adjust(param: AdjustParam) extends UpdateFunc(param) {

  val log: Log = LogFactory.getLog(this.getClass)

  def this() = this(null)

  def this(matrixId: Int,
           src: Array[Int],
           dst: Array[Int],
           seed: Int,
           numNegSample: Int,
           maxIndex: Int,
           partDim: Int,
           grad: Array[Float],
           order: Int) = this(new AdjustParam(matrixId, src, dst, seed, numNegSample, maxIndex, partDim, grad, order))

  override def partitionUpdate(pParam: PartitionUpdateParam) {
    val part = psContext.getMatrixStorageManager.getPart(pParam.getMatrixId, pParam.getPartKey.getPartitionId)
    if (part != null) {
      val p = pParam.asInstanceOf[AdjustPartitionParam]
      val pw = new PartitionWrapper(part, p.partDim, p.order)
      doProcess(pw, p.src, p.dst, p.seed, p.numNegSample, p.maxIndex, p.partDim, p.grad)
    }
  }

  private def doProcess(part: PartitionWrapper,
                        src: Array[Int],
                        dst: Array[Int],
                        seed: Int,
                        numNegSample: Int,
                        maxIndex: Int,
                        dimension: Int,
                        grad: Array[Float]) {
    val curTime = System.currentTimeMillis()
    val sg = new NENegativeSample(maxIndex, seed)
    val inDelta = Array.ofDim[Float](src.length * 2, dimension)
    val outDelta = Array.ofDim[Float](grad.length, dimension)
    val dstIndices = Array.ofDim[Int](grad.length)
    var pos = 0

    for (i <- inDelta.indices) {
      val (srcIdx, dstIdx) = if (i % 2 == 0) (src(i / 2), dst(i / 2)) else (dst(i / 2), src(i / 2))
      dstIndices(pos) = dstIdx
      part.axpy(grad(pos), dstIdx, isInputVec = false, inDelta(i))
      part.axpy(grad(pos), srcIdx, isInputVec = true, outDelta(pos))
      pos += 1
      for (_ <- 0 until numNegSample) {
        val nsIndex = sg.next(srcIdx)
        dstIndices(pos) = nsIndex
        part.axpy(grad(pos), nsIndex, isInputVec = false, inDelta(i))
        part.axpy(grad(pos), srcIdx, isInputVec = true, outDelta(pos))
        pos += 1
      }
    }

    for (i <- inDelta.indices)
      part.addToServer(if (i % 2 == 0) src(i / 2) else dst(i / 2), isInputVec = true, inDelta(i))

    for (i <- outDelta.indices) {
      part.addToServer(dstIndices(i), isInputVec = false, outDelta(i))
    }

    if (new Random(seed).nextDouble() < 0.001) {
      log.info("adjust time cost: " + (System.currentTimeMillis() - curTime) + "ms")
    }
  }
}

object Adjust {

  class AdjustPartitionParam(matrixId: Int,
                             partKey: PartitionKey,
                             var src: Array[Int],
                             var dst: Array[Int],
                             var seed: Int,
                             var numNegSample: Int,
                             var maxIndex: Int,
                             var partDim: Int,
                             var grad: Array[Float],
                             var order: Int) extends PartitionUpdateParam(matrixId, partKey) {
    def this() = this(-1, null, null, null, -1, -1, -1, -1, null, -1)

    override def serialize(buf: ByteBuf): Unit = {
      super.serialize(buf)
      buf.writeInt(src.length)
      src.indices.foreach { i =>
        buf.writeInt(src(i))
        buf.writeInt(dst(i))
      }
      buf.writeInt(seed)
      buf.writeInt(numNegSample)
      buf.writeInt(maxIndex)
      buf.writeInt(partDim)
      buf.writeInt(grad.length)
      grad.foreach(buf.writeFloat)
      buf.writeInt(order)
    }

    override def deserialize(buf: ByteBuf): Unit = {
      super.deserialize(buf)
      val size = buf.readInt
      this.src = new Array[Int](size)
      this.dst = new Array[Int](size)
      this.src.indices.foreach { i =>
        src(i) = buf.readInt
        dst(i) = buf.readInt
      }
      this.seed = buf.readInt
      this.numNegSample = buf.readInt
      this.maxIndex = buf.readInt
      this.partDim = buf.readInt
      this.grad = new Array[Float](buf.readInt)
      grad.indices.foreach(grad(_) = buf.readFloat())
      this.order = buf.readInt
    }

    override def bufferLen: Int = super.bufferLen + src.length * 8 + grad.length * 4 + 28
  }


  class AdjustParam(matrixId: Int,
                    src: Array[Int],
                    dst: Array[Int],
                    seed: Int,
                    numNegSample: Int,
                    maxIndex: Int,
                    dimension: Int,
                    grad: Array[Float],
                    order: Int) extends UpdateParam(matrixId) {
    override def split: java.util.List[PartitionUpdateParam] = {
      PSAgentContext.get.getMatrixMetaManager.getPartitions(matrixId)
        .map(new AdjustPartitionParam(matrixId, _, src, dst, seed, numNegSample, maxIndex, dimension, grad, order))
    }
  }

}
