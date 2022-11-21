package com.tencent.angel.graph.reindex

import java.util
import com.tencent.angel.PartitionKey
import com.tencent.angel.common.ByteBufSerdeUtils
import com.tencent.angel.graph.utils.GraphMatrixUtils
import com.tencent.angel.ml.matrix.psf.get.base._
import com.tencent.angel.psagent.PSAgentContext
import io.netty.buffer.ByteBuf
import scala.collection.JavaConversions._

class InitCheck(param: InitCheckParam) extends GetFunc(param) {

  def this() = this(null)

  /**
    * Partition get. This function is called on PS.
    *
    * @param partParam the partition parameter
    * @return the partition result
    */
  override def partitionGet(partParam: PartitionGetParam): PartitionGetResult = {
    val getInitCheckParam = partParam.asInstanceOf[PartInitCheckParam]
    val row = GraphMatrixUtils.getPSAnyKeyRow(psContext, getInitCheckParam)

    val partResult: Int = row.getStorage.size()

    new PartInitCheckResult(getInitCheckParam.getPartKey, partResult)
  }

  /**
    * Merge the partition get results. This function is called on PSAgent.
    *
    * @param partResults the partition results
    * @return the merged result
    */
  override def merge(partResults: util.List[PartitionGetResult]): GetResult = {
    var total = 0
    for (partResult <- partResults) {
      total += partResult.asInstanceOf[PartInitCheckResult].rowSize
    }
    new GetInitCheckResult(total)
  }
}

class GetInitCheckResult(total: Int) extends GetResult {
  def getResult: Int = total
}

class InitCheckParam(matrixId: Int) extends GetParam(matrixId) {

  /**
    * Split list.
    *
    * @return the list
    */
  override def split(): util.List[PartitionGetParam] = {

    val matrixMeta = PSAgentContext.get().getMatrixMetaManager.getMatrixMeta(matrixId)
    val parts = matrixMeta.getPartitionKeys

    val partParams = new util.ArrayList[PartitionGetParam](parts.length)
    for (index <- (0 until parts.length)) {
      partParams.add(
        new PartInitCheckParam(matrixId, parts(index)))
    }
    partParams
  }
}

class PartInitCheckParam(matrixId: Int,
                         part: PartitionKey) extends PartitionGetParam(matrixId, part) {

  def this() = this(-1, null)

  override def serialize(buf: ByteBuf): Unit = {
    super.serialize(buf)
  }

  override def deserialize(buf: ByteBuf): Unit = {
    super.deserialize(buf)
  }

  override def bufferLen(): Int = {
    super.bufferLen()
  }
}

class PartInitCheckResult(var part: PartitionKey, var rowSize: Int) extends PartitionGetResult {

  def this() = this(null, -1)

  /**
    * Serialize object to the Output stream.
    *
    * @param output the Netty ByteBuf
    */
  override def serialize(output: ByteBuf): Unit = {
    output.writeInt(rowSize)
  }

  /**
    * Deserialize object from the input stream.
    *
    * @param input the input stream
    */
  override def deserialize(input: ByteBuf): Unit = {
    rowSize = input.readInt()
  }

  /**
    * Estimate serialized data size of the object, it used to ByteBuf allocation.
    *
    * @return int serialized data size of the object
    */
  override def bufferLen(): Int = ByteBufSerdeUtils.INT_LENGTH
}
