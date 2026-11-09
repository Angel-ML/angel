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

package com.tencent.angel.spark.ml.graph.algo.embedding.line3

import java.util
import java.util.Random

import com.tencent.angel.PartitionKey
import com.tencent.angel.common.ByteBufSerdeUtils
import com.tencent.angel.ml.matrix.psf.get.base._
import com.tencent.angel.psagent.PSAgentContext
import io.netty.buffer.ByteBuf

import scala.collection.convert.ImplicitConversions._

/**
  * A PS funtion to sample edges with weights on PS
  *
  * @param param function param
  */
class SampleWithWeight(param: SampleWithWeightParam) extends GetFunc(param) {

  def this() = this(null)

  /**
    * Partition get. This function is called on PS.
    *
    * @param partParam the partition parameter
    * @return the partition result
    */
  override def partitionGet(partParam: PartitionGetParam): PartitionGetResult = {
    val sampleParam = partParam.asInstanceOf[PartSampleWithWeightParam]
    val matrix = psContext.getMatrixStorageManager.getMatrix(sampleParam.getMatrixId)
    val part = matrix.getPartition(sampleParam.getPartKey.getPartitionId).asInstanceOf[EdgeAliasTablePartition]
    val samples = part.batchSample(sampleParam.number)
    new PartSampleWithWeightResult(samples._1, samples._2)
  }

  /**
    * Merge the partition get results. This function is called on PSAgent.
    *
    * @param partResults the partition results
    * @return the merged result
    */
  override def merge(partResults: util.List[PartitionGetResult]): GetResult = {
    var len = 0
    partResults.foreach(e => len += e.asInstanceOf[PartSampleWithWeightResult].srcNodes.length)

    val srcNodes = new Array[ReadOnlyByteArray](len)
    val dstNodes = new Array[ReadOnlyByteArray](len)
    var startIndex = 0
    partResults.foreach(e => {
      val sampeles = e.asInstanceOf[PartSampleWithWeightResult]
      System.arraycopy(sampeles.srcNodes, 0, srcNodes, startIndex, sampeles.srcNodes.length)
      System.arraycopy(sampeles.dstNodes, 0, dstNodes, startIndex, sampeles.dstNodes.length)
      startIndex += sampeles.srcNodes.length
    })

    new SampleWithWeightResult(srcNodes, dstNodes)
  }
}

/**
  * Function parameter
  *
  * @param matrixId        alias table matrix id
  * @param sampleNum       sample number
  * @param sampleBatchSize sample batch size, unused now
  * @param aliasTable      alias table to choose the ps partition
  */
class SampleWithWeightParam(matrixId: Int,
                            sampleNum: Int,
                            sampleBatchSize: Int,
                            aliasTable: PSPartitionAliasTable) extends GetParam {
  override def split: util.List[PartitionGetParam] = {
    val parts = PSAgentContext.get.getMatrixMetaManager.getPartitions(matrixId)
    val size = parts.size

    val partParams = new util.ArrayList[PartitionGetParam](size)

    // Sample the partitions for each sample
    var partSamples = samples(sampleNum, aliasTable)

    // Sorts the part ids
    partSamples = partSamples.sorted

    var partId = partSamples(0)
    var lastIndex = 0

    // Counter the sample number for each partition
    for (index <- (0 until partSamples.length)) {
      if (partSamples(index) != partId) {
        partParams.add(
          new PartSampleWithWeightParam(matrixId, parts(partId), index - lastIndex))
        lastIndex = index
        partId = partSamples(index)
      }
    }

    if (lastIndex != partSamples.length) {
      partParams.add(
        new PartSampleWithWeightParam(matrixId, parts(partSamples(lastIndex)), partSamples.length - lastIndex))
    }

    partParams
  }

  def samples(num: Int, aliasTable: PSPartitionAliasTable) = {
    val samples = aliasTable.batchSample(new Random(this.hashCode()), num)
    //val counter = new Int2IntOpenHashMap()
    //samples.foreach(e => counter.addTo(e, 1))
    //counter.foreach(e => println(s"key=${e._1}, value=${e._2}"))
    samples
  }
}

class PartSampleWithWeightParam(
                                 matrixId: Int,
                                 part: PartitionKey,
                                 var number: Int) extends PartitionGetParam(matrixId, part) {

  def this() = this(-1, null, -1)

  override def serialize(output: ByteBuf): Unit = {
    super.serialize(output)
    ByteBufSerdeUtils.serializeInt(output, number)
  }

  override def deserialize(input: ByteBuf): Unit = {
    super.deserialize(input)
    number = ByteBufSerdeUtils.deserializeInt(input)
  }

  override def bufferLen: Int = {
    super.bufferLen() + ByteBufSerdeUtils.INT_LENGTH
  }
}

class SampleWithWeightResult(srcNodes: Array[ReadOnlyByteArray],
                             dstNodes: Array[ReadOnlyByteArray]) extends GetResult {
  def getSrcNodes = srcNodes

  def getDstNodes = dstNodes
}

class PartSampleWithWeightResult(var srcNodes: Array[ReadOnlyByteArray],
                                 var dstNodes: Array[ReadOnlyByteArray]) extends PartitionGetResult {

  def this() = this(null, null)

  /**
    * Serialize object to the Output stream.
    *
    * @param output the Netty ByteBuf
    */
  override def serialize(output: ByteBuf): Unit = {
    ByteBufSerdeUtils.serializeInt(output, srcNodes.length)
    srcNodes.foreach(e => e.serialize(output))

    ByteBufSerdeUtils.serializeInt(output, dstNodes.length)
    dstNodes.foreach(e => e.serialize(output))
  }

  /**
    * Deserialize object from the input stream.
    *
    * @param input the input stream
    */
  override def deserialize(input: ByteBuf): Unit = {
    var len = ByteBufSerdeUtils.deserializeInt(input)
    srcNodes = new Array[ReadOnlyByteArray](len)
    (0 until len).foreach(index => {
      srcNodes(index) = new ReadOnlyByteArray()
      srcNodes(index).deserialize(input)
    }
    )

    len = ByteBufSerdeUtils.deserializeInt(input)
    dstNodes = new Array[ReadOnlyByteArray](len)
    (0 until len).foreach(index => {
      dstNodes(index) = new ReadOnlyByteArray()
      dstNodes(index).deserialize(input)
    }
    )
  }

  /**
    * Estimate serialized data size of the object, it used to ByteBuf allocation.
    *
    * @return int serialized data size of the object
    */
  override def bufferLen(): Int = {
    var len = ByteBufSerdeUtils.INT_LENGTH * 2
    srcNodes.foreach(e => len += e.bufferLen())
    dstNodes.foreach(e => len += e.bufferLen())
    len
  }
}
