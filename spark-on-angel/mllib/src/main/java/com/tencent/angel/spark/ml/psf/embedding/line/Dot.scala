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

import java.text.SimpleDateFormat
import java.util.Date
import scala.collection.JavaConversions._
import scala.util.Random

import io.netty.buffer.ByteBuf
import org.apache.commons.logging.{Log, LogFactory}

import com.tencent.angel.PartitionKey
import com.tencent.angel.ml.matrix.psf.get.base.{GetParam, PartitionGetParam}
import com.tencent.angel.ps.storage.matrix.ServerPartition
import com.tencent.angel.psagent.PSAgentContext
import com.tencent.angel.spark.ml.psf.embedding.line.Dot.{DotParam, DotPartitionParam}
import com.tencent.angel.spark.ml.psf.embedding.{NEDot, NENegativeSample, PartitionWrapper}

class Dot(param: GetParam) extends NEDot(param) {

  val log: Log = LogFactory.getLog(this.getClass)

  def this(matrixId: Int,
           src: Array[Int],
           dst: Array[Int],
           seed: Int,
           numNegSample: Int,
           maxIndex: Int,
           parDim: Int,
           order: Int) =
    this(new DotParam(matrixId, src, dst, seed, numNegSample, maxIndex, parDim, order))

  def this() = this(null)

  override def doProcess(part: ServerPartition, pParam: PartitionGetParam): Array[Float] = {
    val partParam = pParam.asInstanceOf[DotPartitionParam]
    assert(partParam.src.length == partParam.dst.length)
    val pw = new PartitionWrapper(part, partParam.partDim, partParam.order)
    val src = partParam.src
    val dst = partParam.dst
    val numNegSample = partParam.numNegSample
    val sg = new NENegativeSample(partParam.maxIndex, partParam.seed)

    val startTime = System.currentTimeMillis()
    val dotRet = new Array[Float](src.length * 2 * (1 + numNegSample))
    var pos = 0
    for (i <- 0 until src.length * 2) {
      val (srcIdx, dstIdx) = if (i % 2 == 0) (src(i / 2), dst(i / 2)) else (dst(i / 2), src(i / 2))
      dotRet(pos) = pw.dot(srcIdx, dstIdx)
      pos += 1
      for (_ <- 0 until numNegSample) {
        dotRet(pos) = pw.dot(srcIdx, sg.next(srcIdx))
        pos += 1
      }
    }
    if (Random.nextDouble() < 0.001) logTime(startTime)
    dotRet
  }

  private def logTime(startTime: Long): Unit = {
    val start = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date(startTime))
    val cost = System.currentTimeMillis() - startTime
    log.info(s"dot start in $start, time cost: ${cost}ms")
  }
}

object Dot {

  class DotPartitionParam(matrixId: Int,
                          partKey: PartitionKey,
                          var src: Array[Int],
                          var dst: Array[Int],
                          var seed: Int,
                          var numNegSample: Int,
                          var maxIndex: Int,
                          var partDim: Int,
                          var order: Int) extends PartitionGetParam(matrixId, partKey) {
    def this() = this(-1, null, null, null, -1, -1, -1, -1, -1)

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
      buf.writeInt(order)
    }

    override def deserialize(buf: ByteBuf): Unit = {
      super.deserialize(buf)
      val length = buf.readInt
      this.src = new Array[Int](length)
      this.dst = new Array[Int](length)
      src.indices.foreach { i =>
        this.src(i) = buf.readInt()
        this.dst(i) = buf.readInt()
      }
      this.seed = buf.readInt
      this.numNegSample = buf.readInt
      this.maxIndex = buf.readInt
      this.partDim = buf.readInt
      this.order = buf.readInt
    }

    override def bufferLen: Int = super.bufferLen + 8 * src.length + 24
  }

  class DotParam(matrixId: Int,
                 var src: Array[Int],
                 var dst: Array[Int],
                 var seed: Int,
                 var numNegSample: Int,
                 var maxIndex: Int,
                 var vecNumPreNum: Int,
                 var order: Int) extends GetParam(matrixId) {

    override def split: java.util.List[PartitionGetParam] = {
      PSAgentContext.get.getMatrixMetaManager.getPartitions(matrixId)
        .map(new DotPartitionParam(matrixId, _, src, dst, seed, numNegSample, maxIndex, vecNumPreNum, order))
    }
  }

}
