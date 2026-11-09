package com.tencent.angel.spark.ml.graph.algo.embedding.line3

import java.util

import com.tencent.angel.PartitionKey
import com.tencent.angel.common.ByteBufSerdeUtils
import com.tencent.angel.ml.matrix.psf.get.base._
import com.tencent.angel.psagent.PSAgentContext
import com.tencent.angel.psagent.matrix.transport.router.hash.{HashAnyKeysAnyValuesPart, HashIntKeysIntValuesPart, HashIntKeysPart}
import com.tencent.angel.psagent.matrix.transport.router.{KeyPart, KeyValuePart}
import io.netty.buffer.ByteBuf

import scala.util.Random

class NegativeSample(param: NegativeSampleParam) extends GetFunc(param) {

  def this() = this(null)

  /**
    * Partition get. This function is called on PS.
    *
    * @param partParam the partition parameter
    * @return the partition result
    */
  override def partitionGet(partParam: PartitionGetParam): PartitionGetResult = {
    val sampleParam = partParam.asInstanceOf[PartNegativeSampleParam]
    val matrix = psContext.getMatrixStorageManager.getMatrix(sampleParam.getMatrixId)
    val part = matrix.getPartition(sampleParam.getPartKey.getPartitionId).asInstanceOf[INodeTableStorageOp]

    val srcNodes = sampleParam.split.asInstanceOf[HashAnyKeysAnyValuesPart].getKeys
    val targetNodes = sampleParam.split.asInstanceOf[HashAnyKeysAnyValuesPart].getValues

    val samples = part.negativeSample(srcNodes, targetNodes, sampleParam.count)
    new PartNegativeSampleResult(sampleParam.getPartKey.getPartitionId, samples)
  }

  /**
    * Merge the partition get results. This function is called on PSAgent.
    *
    * @param partResults the partition results
    * @return the merged result
    */
  override def merge(partResults: util.List[PartitionGetResult]): GetResult = {
    val result = new Array[Array[ReadOnlyByteArray]](param.srcNodes.length)
    val partSize = partResults.size()
    for (partIndex <- (0 until partSize)) {
      val samplePartResult = partResults.get(partIndex).asInstanceOf[PartNegativeSampleResult]
      val indices = param.nodeIndices(samplePartResult.partId)
      val partSamples = samplePartResult.batchSamples
      for (nodeIndex <- (0 until indices.length)) {
        result(indices(nodeIndex)) = partSamples(nodeIndex)
      }
    }

    NegativeSampleResult(result)
  }
}

case class NegativeSampleResult(val batchSamples: Array[Array[ReadOnlyByteArray]]) extends GetResult

class PartNegativeSampleResult(var partId: Int,
                               var batchSamples: Array[Array[ReadOnlyByteArray]]) extends PartitionGetResult {

  def this() = this(-1, null)

  /**
    * Serialize object to the Output stream.
    *
    * @param output the Netty ByteBuf
    */
  override def serialize(output: ByteBuf): Unit = {
    ByteBufSerdeUtils.serializeInt(output, partId)
    ByteBufSerdeUtils.serializeInt(output, batchSamples.length)
    batchSamples.foreach(samples => {
      ByteBufSerdeUtils.serializeInt(output, samples.length)
      samples.foreach(sample => {
        sample.serialize(output)
      })
    })
  }

  /**
    * Deserialize object from the input stream.
    *
    * @param input the input stream
    */
  override def deserialize(input: ByteBuf): Unit = {
    partId = ByteBufSerdeUtils.deserializeInt(input)
    val nodeNum = ByteBufSerdeUtils.deserializeInt(input)
    batchSamples = new Array[Array[ReadOnlyByteArray]](nodeNum)

    for (nodeIndex <- (0 until nodeNum)) {
      val sampleNum = ByteBufSerdeUtils.deserializeInt(input)
      batchSamples(nodeIndex) = new Array[ReadOnlyByteArray](sampleNum)
      for (sampleIndex <- (0 until sampleNum)) {
        batchSamples(nodeIndex)(sampleIndex) = new ReadOnlyByteArray()
        batchSamples(nodeIndex)(sampleIndex).deserialize(input)
      }
    }
  }

  /**
    * Estimate serialized data size of the object, it used to ByteBuf allocation.
    *
    * @return int serialized data size of the object
    */
  override def bufferLen(): Int = {
    var len = ByteBufSerdeUtils.INT_LENGTH * 2
    batchSamples.foreach(samples => {
      len += ByteBufSerdeUtils.INT_LENGTH
      samples.foreach(sample => {
        len += sample.bufferLen()
      })
    })

    len
  }
}

class NegativeSampleParam(matrixId: Int,
                          val srcNodes: Array[ReadOnlyByteArray],
                          val dstNodes: Array[ReadOnlyByteArray],
                          val count: Int) extends GetParam(matrixId) {
  @transient
  @volatile
  var nodeIndices: Array[Array[Int]] = _

  override def split(): util.List[PartitionGetParam] = {
    val matrixMeta = PSAgentContext.get().getMatrixMetaManager.getMatrixMeta(matrixId)
    val parts = matrixMeta.getPartitionKeys
    val splits = new Array[KeyValuePart](parts.length)
    val indices = new Array[KeyPart](parts.length)
    nodeIndices = new Array[Array[Int]](parts.length)

    for (index <- (0 until parts.length)) {
      splits(index) = new HashAnyKeysAnyValuesPart(srcNodes.length / parts.length)
      indices(index) = new HashIntKeysPart(srcNodes.length / parts.length)
    }

    val r = new Random(System.currentTimeMillis())
    for (index <- (0 until srcNodes.length)) {
      val partId = r.nextInt(splits.length)
      splits(partId).asInstanceOf[HashAnyKeysAnyValuesPart].add(srcNodes(index), dstNodes(index))
      indices(partId).asInstanceOf[HashIntKeysPart].add(index)
    }

    val partParams = new util.ArrayList[PartitionGetParam](parts.length)
    for (index <- (0 until parts.length)) {
      if (splits(index) != null && splits(index).size() > 0) {
        partParams.add(new PartNegativeSampleParam(matrixId, parts(index), splits(index), count))
        nodeIndices(index) = indices(index).asInstanceOf[HashIntKeysPart].getKeys
      }
    }

    partParams
  }
}

class PartNegativeSampleParam(matrixId: Int,
                              part: PartitionKey,
                              var split: KeyValuePart,
                              var count: Int) extends PartitionGetParam(matrixId, part) {
  def this() = this(-1, null, null, -1)

  override def serialize(buf: ByteBuf): Unit = {
    super.serialize(buf)
    ByteBufSerdeUtils.serializeKeyValuePart(buf, split)
    ByteBufSerdeUtils.serializeInt(buf, count)
  }

  override def deserialize(buf: ByteBuf): Unit = {
    super.deserialize(buf)
    split = ByteBufSerdeUtils.deserializeKeyValuePart(buf)
    count = ByteBufSerdeUtils.deserializeInt(buf)
  }

  override def bufferLen(): Int = {
    var len = super.bufferLen()
    len += ByteBufSerdeUtils.serializedKeyValuePartLen(split)
    len += ByteBufSerdeUtils.INT_LENGTH
    len
  }
}
