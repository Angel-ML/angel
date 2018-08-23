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

import scala.collection.JavaConversions._
import scala.collection.mutable.ArrayBuffer

import io.netty.buffer.ByteBuf
import org.apache.commons.logging.{Log, LogFactory}

import com.tencent.angel.PartitionKey
import com.tencent.angel.ml.matrix.psf.get.base._
import com.tencent.angel.ps.storage.matrix.ServerPartition
import com.tencent.angel.psagent.PSAgentContext
import com.tencent.angel.spark.ml.psf.embedding.word2vec.Dot.{DotParam, DotProbPartitionParam}
import com.tencent.angel.spark.ml.psf.embedding.{NEDot, NENegativeSample, PartitionWrapper}


class Dot(param: GetParam) extends NEDot(param) {
  val log: Log = LogFactory.getLog(this.getClass)

  def this(matrixId: Int,
           sentences: Array[Array[Int]],
           seed: Int,
           ns: Int,
           window: Int,
           maxIndex: Int,
           partDim: Int) = this(new DotParam(matrixId, sentences, seed, ns, window, maxIndex, partDim))

  def this() = this(null)

  override def doProcess(part: ServerPartition, pParam: PartitionGetParam): Array[Float] = {
    val partParam = pParam.asInstanceOf[DotProbPartitionParam]
    val order = 2
    val pw = new PartitionWrapper(part, partParam.partDim, order)
    val sentences = partParam.sentences
    val ns = partParam.ns
    val window = partParam.window
    val sg = new NENegativeSample(partParam.maxIndex, partParam.seed)
    val dotRet = new ArrayBuffer[Float]

    for (sentence <- sentences) {
      for (i <- sentence.indices) {
        val input = sentence(i)
        val outputs = Utils.getSentenceContext(sentence, i, window)
        for (output <- outputs) {
          dotRet += pw.dot(input, output)
          for (_ <- 0 until ns)
            dotRet += pw.dot(input, sg.next(input))
        }
      }
    }
    dotRet.toArray
  }
}

object Dot {

  class DotProbPartitionParam(matrixId: Int,
                              partKey: PartitionKey,
                              var sentences: Array[Array[Int]],
                              var seed: Int,
                              var ns: Int,
                              var window: Int,
                              var maxIndex: Int,
                              var partDim: Int) extends PartitionGetParam(matrixId, partKey) {
    def this() = this(-1, null, null, -1, -1, -1, -1, -1)

    override def serialize(buf: ByteBuf): Unit = {
      super.serialize(buf)
      buf.writeInt(sentences.length)
      for (sentence <- sentences) {
        buf.writeInt(sentence.length)
        sentence.foreach(buf.writeInt)
      }
      buf.writeInt(seed)
      buf.writeInt(ns)
      buf.writeInt(window)
      buf.writeInt(maxIndex)
      buf.writeInt(partDim)
    }

    override def deserialize(buf: ByteBuf): Unit = {
      super.deserialize(buf)
      this.sentences = new Array[Array[Int]](buf.readInt)
      this.sentences.indices.foreach { i =>
        this.sentences(i) = Array.fill(buf.readInt())(buf.readInt())
      }
      this.seed = buf.readInt
      this.ns = buf.readInt
      this.window = buf.readInt
      this.maxIndex = buf.readInt
      this.partDim = buf.readInt
    }

    override def bufferLen: Int = super.bufferLen + sentences.map(_.length + 1).sum + 24
  }

  class DotParam(matrixId: Int,
                 var sentences: Array[Array[Int]],
                 var seed: Int,
                 var ns: Int,
                 var window: Int,
                 var maxIndex: Int,
                 var partDim: Int) extends GetParam(matrixId) {
    override def split: java.util.List[PartitionGetParam] = {
      PSAgentContext.get.getMatrixMetaManager.getPartitions(matrixId)
        .map(new DotProbPartitionParam(matrixId, _, sentences, seed, ns, window, maxIndex, partDim))
    }
  }

}
