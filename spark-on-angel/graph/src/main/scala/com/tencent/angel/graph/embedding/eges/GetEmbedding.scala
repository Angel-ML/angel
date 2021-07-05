package com.tencent.angel.graph.embedding.eges

import java.util

import com.tencent.angel.PartitionKey
import com.tencent.angel.common.ByteBufSerdeUtils
import com.tencent.angel.graph.data.NodeUtils
import com.tencent.angel.graph.utils.GraphMatrixUtils
import com.tencent.angel.ml.matrix.psf.get.base._
import com.tencent.angel.psagent.PSAgentContext
import com.tencent.angel.psagent.matrix.transport.router.{KeyPart, RouterUtils}
import com.tencent.angel.psagent.matrix.transport.router.operator.IIntKeyPartOp
import io.netty.buffer.ByteBuf
import it.unimi.dsi.fastutil.ints.Int2ObjectOpenHashMap

import scala.collection.JavaConversions._

class GetEmbedding(param: EmbeddingGetParam) extends GetFunc(param) {

  def this() = this(null)

  /**
   * Partition get. This function is called on PS.
   *
   * @param partParam the partition parameter
   * @return the partition result
   */
  override def partitionGet(partParam: PartitionGetParam): PartitionGetResult = {
    val getEmbeddingParam = partParam.asInstanceOf[PartEmbeddingGetParam]
    val row = GraphMatrixUtils.getPSIntKeyRow(psContext, getEmbeddingParam)

    val NodeData = getEmbeddingParam.nodeIds
    var NodeIds: Array[Int] = null
    if (NodeData != null) {
      NodeIds = NodeData.asInstanceOf[IIntKeyPartOp].getKeys
    }

    var feats: Int2ObjectOpenHashMap[Array[Float]] = null
    if(NodeIds != null) {
      feats = new Int2ObjectOpenHashMap[Array[Float]](NodeIds.length)
      for (nodeId <- NodeIds) {
        feats.put(nodeId, row.get(nodeId).asInstanceOf[EmbeddingNode].getFeats)
      }
    }

    new PartEmbeddingGetResult(getEmbeddingParam.getPartKey, feats)

  }

  /**
   * Merge the partition get results. This function is called on PSAgent.
   *
   * @param partResults the partition results
   * @return the merged result
   */
  override def merge(partResults: util.List[PartitionGetResult]): GetResult = {
    val Feats = new Int2ObjectOpenHashMap[Array[Float]](param.NodeNum)

    for (partResult <- partResults) {
      if(partResult.asInstanceOf[PartEmbeddingGetResult].feats != null) {
        Feats.putAll(partResult.asInstanceOf[PartEmbeddingGetResult].feats)
      }

    }
    new EmbeddingGetResult(Feats)
  }
}

class EmbeddingGetResult(feats: Int2ObjectOpenHashMap[Array[Float]]) extends GetResult {
  def getResult = feats
}

class EmbeddingGetParam(matrixId: Int, Nodes: Array[Int]) extends GetParam(matrixId) {
  var NodeNum = 0

  /**
   * Split list.
   *
   * @return the list
   */
  override def split(): util.List[PartitionGetParam] = {
    NodeNum = Nodes.length

    val matrixMeta = PSAgentContext.get().getMatrixMetaManager.getMatrixMeta(matrixId)
    val parts = matrixMeta.getPartitionKeys

    val splits = RouterUtils.split(matrixMeta, 0, Nodes.clone(), false)

    val partParams = new util.ArrayList[PartitionGetParam](parts.length)
    for (index <- 0 until parts.length) {
      if (splits(index) != null && splits(index).size() > 0) {
        partParams.add(new PartEmbeddingGetParam(matrixId, parts(index), splits(index)))
      }
    }

    partParams
  }

}

class PartEmbeddingGetParam(matrixId: Int, part: PartitionKey, var nodeIds: KeyPart)
  extends PartitionGetParam(matrixId, part) {

  def this() = this(-1, null, null)

  override def serialize(buf: ByteBuf): Unit = {
    super.serialize(buf)

    if (nodeIds != null) {
      ByteBufSerdeUtils.serializeBoolean(buf, true)
      ByteBufSerdeUtils.serializeKeyPart(buf, nodeIds)
    } else {
      ByteBufSerdeUtils.serializeBoolean(buf, false)
    }

  }

  override def deserialize(buf: ByteBuf): Unit = {
    super.deserialize(buf)

    if (ByteBufSerdeUtils.deserializeBoolean(buf)) {
      nodeIds = ByteBufSerdeUtils.deserializeKeyPart(buf)
    }

  }

  override def bufferLen(): Int = {
    var len = super.bufferLen()
    len += ByteBufSerdeUtils.BOOLEN_LENGTH
    if (nodeIds != null) {
      len += ByteBufSerdeUtils.serializedKeyPartLen(nodeIds)
    }

    len
  }

}

class PartEmbeddingGetResult(var part: PartitionKey,
                             var feats: Int2ObjectOpenHashMap[Array[Float]])
  extends PartitionGetResult {

  def this() = this(null, null)

  /**
   * Serialize object to the Output stream.
   *
   * @param output the Netty ByteBuf
   */
  override def serialize(output: ByteBuf): Unit = {
    if(feats != null) {
      output.writeInt(feats.size())

      val resIter = feats.int2ObjectEntrySet().fastIterator()
      while(resIter.hasNext) {
        val entry = resIter.next()
        output.writeInt(entry.getIntKey)
        NodeUtils.serialize(entry.getValue, output)
      }
    } else {
      output.writeInt(0);
    }

  }

  /**
   * Deserialize object from the input stream.
   *
   * @param input the input stream
   */
  override def deserialize(input: ByteBuf): Unit = {
    var len = input.readInt()
    if(len > 0) {
      feats = new Int2ObjectOpenHashMap[Array[Float]](len)
      for (i <- 0 until len) {
        feats.put(input.readInt(), NodeUtils.deserializeFloats(input))
      }
    }

  }

  /**
   * Estimate serialized data size of the object, it used to ByteBuf allocation.
   *
   * @return int serialized data size of the object
   */
  override def bufferLen(): Int = {
    var len = 4

    var elemLen = 0
    if(feats != null) {
      val resIter = feats.int2ObjectEntrySet().fastIterator()
      var break = false
      while(resIter.hasNext && !break) {
        val entry = resIter.next()
        elemLen = 4 + NodeUtils.dataLen(entry.getValue)
        break = true
      }

      len += elemLen * feats.size()
    }

    len
  }


}