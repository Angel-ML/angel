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


package com.tencent.angel.spark.ml.psf.embedding.word2vec

import java.util.Random
import scala.collection.JavaConversions._

import io.netty.buffer.ByteBuf
import org.apache.commons.logging.{Log, LogFactory}

import com.tencent.angel.PartitionKey
import com.tencent.angel.ml.matrix.psf.update.base.{PartitionUpdateParam, UpdateFunc, UpdateParam}
import com.tencent.angel.psagent.PSAgentContext
import com.tencent.angel.spark.ml.psf.embedding.word2vec.Adjust.{AdjustParam, AdjustPartitionParam}
import com.tencent.angel.spark.ml.psf.embedding.{NENegativeSample, PartitionWrapper}


class Adjust(param: UpdateParam) extends UpdateFunc(param) {
  val log: Log = LogFactory.getLog(this.getClass)

  def this(matrixId: Int,
           sentences: Array[Array[Int]],
           seed: Int,
           ns: Int,
           window: Int,
           maxIndex: Int,
           partDim: Int,
           grad: Array[Float]) = this(new AdjustParam(matrixId, sentences, seed, ns, window, maxIndex, partDim, grad))

  def this() = this(null)

  override def partitionUpdate(pParam: PartitionUpdateParam): Unit = {
    val part = psContext.getMatrixStorageManager.getPart(pParam.getMatrixId, pParam.getPartKey.getPartitionId)
    if (part == null) return
    val partParam = pParam.asInstanceOf[AdjustPartitionParam]
    val sentences = partParam.sentences
    val seed = partParam.seed
    val ns = partParam.ns
    val window = partParam.window
    val grad = partParam.grad
    val maxIndex = partParam.maxIndex
    val dimension = partParam.partDim
    val order = 2
    val pw = new PartitionWrapper(part, dimension, order)
    doProcess(pw, sentences, seed, ns, window, maxIndex, dimension, grad)
  }

  private def doProcess(part: PartitionWrapper,
                        sentences: Array[Array[Int]],
                        seed: Int,
                        ns: Int,
                        window: Int,
                        maxIndex: Int,
                        dimension: Int,
                        grad: Array[Float]): Unit = {
    val curTime = System.currentTimeMillis
    val sg = new NENegativeSample(maxIndex, seed)
    var inWordsNum = 0
    for (sentence <- sentences) {
      inWordsNum += sentence.length
    }
    val inDelta = Array.ofDim[Float](inWordsNum, dimension)
    val outDelta = Array.ofDim[Float](grad.length, dimension)
    val dstIndices = new Array[Int](grad.length)
    var inWordPos = 0
    var gradPos = 0
    for (sentence <- sentences) {
      for (i <- sentence.indices) {
        val input = sentence(i)
        val outputs = Utils.getSentenceContext(sentence, i, window)
        for (output <- outputs) {
          dstIndices(gradPos) = output
          part.axpy(grad(gradPos), output, isInputVec = false, inDelta(inWordPos))
          part.axpy(grad(gradPos), input, isInputVec = true, outDelta(gradPos))
          gradPos += 1
          for (_ <- 0 until ns) {
            val nsIndex = sg.next(input)
            dstIndices(gradPos) = nsIndex
            part.axpy(grad(gradPos), nsIndex, isInputVec = false, inDelta(inWordPos))
            part.axpy(grad(gradPos), input, isInputVec = true, outDelta(gradPos))
            gradPos += 1
          }
        }
        inWordPos += 1
      }
    }

    inWordPos = 0
    for (sentence <- sentences) {
      for (input <- sentence) {
        part.addToServer(input, isInputVec = true, inDelta(inWordPos))
        inWordPos += 1
      }
    }

    for (i <- outDelta.indices)
      part.addToServer(dstIndices(i), isInputVec = false, outDelta(i))

    if (new Random().nextDouble < 0.001)
      log.info("adjust time cost: " + (System.currentTimeMillis - curTime) + "ms")
  }
}

object Adjust {

  class AdjustPartitionParam(matrixId: Int,
                             partKey: PartitionKey,
                             var sentences: Array[Array[Int]],
                             var seed: Int,
                             var ns: Int,
                             var window: Int,
                             var maxIndex: Int,
                             var partDim: Int,
                             var grad: Array[Float],
                             var bufSize: Int) extends PartitionUpdateParam(matrixId, partKey) {
    def this() = this(-1, null, null, -1, -1, -1, -1, -1, null, -1)

    override def serialize(buf: ByteBuf): Unit = {
      super.serialize(buf)
      buf.writeInt(sentences.length)
      for (ows <- sentences) {
        buf.writeInt(ows.length)
        ows.foreach(buf.writeInt)
      }
      buf.writeInt(seed)
      buf.writeInt(ns)
      buf.writeInt(window)
      buf.writeInt(maxIndex)
      buf.writeInt(partDim)
      buf.writeInt(grad.length)
      grad.foreach(buf.writeFloat)
    }

    override def deserialize(buf: ByteBuf): Unit = {
      super.deserialize(buf)
      this.sentences = new Array[Array[Int]](buf.readInt)
      for (i <- sentences.indices) {
        this.sentences(i) = Array.fill[Int](buf.readInt)(buf.readInt)
      }
      this.seed = buf.readInt
      this.ns = buf.readInt
      this.window = buf.readInt
      this.maxIndex = buf.readInt
      this.partDim = buf.readInt
      this.grad = Array.fill[Float](buf.readInt)(buf.readFloat)
    }

    override def bufferLen: Int = super.bufferLen + bufSize
  }


  class AdjustParam(matrixId: Int,
                    var sentences: Array[Array[Int]],
                    var seed: Int,
                    var ns: Int,
                    var window: Int,
                    var maxIndex: Int,
                    var dimension: Int,
                    var grad: Array[Float]) extends UpdateParam(matrixId) {
    override def split: java.util.List[PartitionUpdateParam] = {
      val bufSize: Int = 20 + (grad.length + 1 + sentences.map(_.length + 1).sum) * 4
      PSAgentContext.get.getMatrixMetaManager.getPartitions(matrixId)
        .map(new AdjustPartitionParam(matrixId, _, sentences, seed, ns, window, maxIndex, dimension, grad, bufSize))
    }
  }

}
