package com.tencent.angel.spark.ml.graph.algo.embedding.line3

import java.util

import com.tencent.angel.PartitionKey
import com.tencent.angel.common.ByteBufSerdeUtils
import com.tencent.angel.graph.utils.GraphMatrixUtils
import com.tencent.angel.ml.matrix.psf.update.base.{PartitionUpdateParam, UpdateFunc, UpdateParam}
import com.tencent.angel.psagent.PSAgentContext
import io.netty.buffer.ByteBuf

class InitNodeTable(param: InitNodeTableParam) extends UpdateFunc(param) {
  def this() = this(null)

  /**
    * Partition update.
    *
    * @param partParam the partition parameter
    */
  override def partitionUpdate(partParam: PartitionUpdateParam): Unit = {
    // Get row from matrix partition that store neighbor table on PS
    val initParam = partParam.asInstanceOf[PartInitNodeTableParam]
    val row = GraphMatrixUtils.getPSAnyKeyRow(psContext, initParam)

    // Get the matrix partition that store alias table on PS
    val nodeTable = psContext.getMatrixStorageManager.getMatrix(initParam.nodeTableId)
    val nodeTablePart = nodeTable.getPartition(initParam.getPartKey.getPartitionId).asInstanceOf[NodeTablePartition]

    val nodeIds = new Array[ReadOnlyByteArray](row.size())
    val iter = row.getStorage.iterator()
    var nodeIndex = 0;
    while(iter.hasNext) {
      nodeIds(nodeIndex) = iter.next().getKey.asInstanceOf[ReadOnlyByteArray]
      nodeIndex += 1
    }

    val offset = row.getStorage.getIndexOffset.toInt
    val storage = new NodeTableStorage(offset, nodeIds)
    nodeTablePart.setStorage(storage)
  }
}

/**
  * Function parameters
  *
  * @param matrixId neighbor table matrix id
  * @param aliasTableId
  */
class InitNodeTableParam(matrixId: Int, nodeTableId: Int) extends UpdateParam(matrixId) {
  /**
    * Split list.
    *
    * @return the list
    */
  override def split(): util.List[PartitionUpdateParam] = {
    val parts = PSAgentContext.get().getMatrixMetaManager.getMatrixMeta(matrixId).getPartitionKeys
    val partParams = new util.ArrayList[PartitionUpdateParam](parts.length)
    parts.foreach(e => {
      partParams.add(new PartInitNodeTableParam(matrixId, e, nodeTableId))
    })

    partParams
  }
}

class PartInitNodeTableParam(
                              matrixId: Int,
                              partKey: PartitionKey,
                              var nodeTableId: Int) extends PartitionUpdateParam(matrixId, partKey) {

  def this() = this(-1, null, -1)

  override def serialize(output: ByteBuf): Unit = {
    super.serialize(output)
    ByteBufSerdeUtils.serializeInt(output, nodeTableId)
  }

  override def deserialize(input: ByteBuf): Unit = {
    super.deserialize(input)
    nodeTableId = ByteBufSerdeUtils.deserializeInt(input)
  }

  override def bufferLen: Int = {
    super.bufferLen() + ByteBufSerdeUtils.INT_LENGTH
  }
}
