package com.tencent.angel.graph.reindex

import java.util
import com.tencent.angel.PartitionKey
import com.tencent.angel.common.ByteBufSerdeUtils
import com.tencent.angel.graph.utils.GraphMatrixUtils
import com.tencent.angel.ml.matrix.psf.get.base._
import com.tencent.angel.ps.storage.vector.element.IElement
import com.tencent.angel.psagent.PSAgentContext
import com.tencent.angel.psagent.matrix.transport.router.operator.IAnyKeyPartOp
import com.tencent.angel.psagent.matrix.transport.router.{KeyPart, RouterUtils}
import io.netty.buffer.ByteBuf
import it.unimi.dsi.fastutil.objects.Object2LongOpenHashMap

import scala.collection.JavaConversions._

class GetNodesMap(param: GetNodesMapParam) extends GetFunc(param) {

  def this() = this(null)

  /**
    * Partition get. This function is called on PS.
    *
    * @param partParam the partition parameter
    * @return the partition result
    */
  override def partitionGet(partParam: PartitionGetParam): PartitionGetResult = {
    val getNodesMapParam = partParam.asInstanceOf[PartGetNodesMapParam]
    val row = GraphMatrixUtils.getPSAnyKeyRow(psContext, getNodesMapParam)

    val data = getNodesMapParam.nodeIds
    var nodeIds: Array[IElement] = null
    if (data != null) {
      nodeIds = data.asInstanceOf[IAnyKeyPartOp].getKeys
    }

    var nodesMapPartResult: Object2LongOpenHashMap[IElement] = null
    if (nodeIds != null) {
      nodesMapPartResult = new Object2LongOpenHashMap[IElement](nodeIds.length)
      for (nodeId <- nodeIds) {
        nodesMapPartResult.put(nodeId, row.get(nodeId).asInstanceOf[ReIndexValue].getValue)
      }
    }

    new PartGetNodesMapResult(getNodesMapParam.getPartKey, nodesMapPartResult)
  }

  /**
    * Merge the partition get results. This function is called on PSAgent.
    *
    * @param partResults the partition results
    * @return the merged result
    */
  override def merge(partResults: util.List[PartitionGetResult]): GetResult = {
    val nodesMapResult = new Object2LongOpenHashMap[ReadOnlyByteArray](param.nodesNum)

    for (partResult <- partResults) {
      if (partResult.asInstanceOf[PartGetNodesMapResult].maps != null) {
        val iter = partResult.asInstanceOf[PartGetNodesMapResult].maps.object2LongEntrySet().fastIterator()
        while (iter.hasNext) {
          val entry = iter.next
          nodesMapResult.put(entry.getKey.asInstanceOf[ReadOnlyByteArray], entry.getLongValue)
        }
      }
    }
    new GetNodesMapResult(nodesMapResult)
  }
}

class GetNodesMapResult(nodesMapResult: Object2LongOpenHashMap[ReadOnlyByteArray]) extends GetResult {
  def getResult: Object2LongOpenHashMap[ReadOnlyByteArray] = nodesMapResult
}

class GetNodesMapParam(matrixId: Int, nodes: Array[ReadOnlyByteArray]) extends GetParam(matrixId) {
  var nodesNum = 0

  /**
    * Split list.
    *
    * @return the list
    */
  override def split(): util.List[PartitionGetParam] = {
    nodesNum = nodes.length

    val matrixMeta = PSAgentContext.get().getMatrixMetaManager.getMatrixMeta(matrixId)
    val parts = matrixMeta.getPartitionKeys

    val splits = RouterUtils.split(matrixMeta, 0, nodes.clone().map(e => e.asInstanceOf[IElement]), false)

    val partParams = new util.ArrayList[PartitionGetParam](parts.length)
    for (index <- (0 until parts.length)) {
      if (splits(index) != null && splits(index).size() > 0) {
        partParams.add(
          new PartGetNodesMapParam(matrixId, parts(index), splits(index)))
      }
    }
    partParams
  }
}

class PartGetNodesMapParam(matrixId: Int,
                           part: PartitionKey,
                           var nodeIds: KeyPart) extends PartitionGetParam(matrixId, part) {

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

class PartGetNodesMapResult(var part: PartitionKey,
                            var maps: Object2LongOpenHashMap[IElement]) extends PartitionGetResult {

  def this() = this(null, null)

  /**
    * Serialize object to the Output stream.
    *
    * @param output the Netty ByteBuf
    */
  override def serialize(output: ByteBuf): Unit = {
    if (maps != null) {
      output.writeInt(maps.size())

      val resIter = maps.object2LongEntrySet().fastIterator()
      while (resIter.hasNext) {
        val entry = resIter.next()
        entry.getKey.serialize(output)
        output.writeLong(entry.getLongValue)
      }
    } else {
      output.writeInt(0)
    }
  }

  /**
    * Deserialize object from the input stream.
    *
    * @param input the input stream
    */
  override def deserialize(input: ByteBuf): Unit = {
    var len = input.readInt()
    if (len > 0) {
      maps = new Object2LongOpenHashMap[IElement](len)
      for (i <- 0 until len) {
        val key = new ReadOnlyByteArray()
        key.deserialize(input)
        maps.put(key,input.readLong())
      }
    }
  }

  /**
    * Estimate serialized data size of the object, it used to ByteBuf allocation.
    *
    * @return int serialized data size of the object
    */
  override def bufferLen(): Int = {
    var len = ByteBufSerdeUtils.INT_LENGTH
    var elemLen = 0
    if (maps != null) {
      val resIter = maps.object2LongEntrySet().fastIterator()
      var break = false
      while (resIter.hasNext && !break) {
        val entry = resIter.next()
        elemLen = entry.getKey.bufferLen() + ByteBufSerdeUtils.LONG_LENGTH
        break = true
      }
      len += elemLen * maps.size()
    }
    len
  }
}
