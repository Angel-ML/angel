package com.tencent.angel.graph.community.hanp

import com.tencent.angel.graph.utils.ModelContextUtils
import com.tencent.angel.ml.math2.vector.{LongFloatVector, LongLongVector, Vector}
import com.tencent.angel.ml.matrix.RowType
import com.tencent.angel.psagent.PSAgentContext
import com.tencent.angel.spark.models.PSVector
import com.tencent.angel.spark.models.impl.PSVectorImpl
import com.tencent.angel.spark.util.VectorUtils
import com.tencent.angel.graph.common.param.ModelContext
import org.apache.spark.SparkContext

class HANPPSModel(var scoreInMsgs: PSVector,
                  var scoreOutMsgs: PSVector,
                  val degreeMsgs: PSVector,
                  var labelInMsgs: PSVector,
                  var labelOutMsgs: PSVector) extends Serializable {
                    val dim: Long = degreeMsgs.dimension

                    def initScore(msgs: Vector): Unit =
                      scoreInMsgs.update(msgs)

                    def readScore(nodes: Array[Long]): LongFloatVector =
                      scoreInMsgs.pull(nodes).asInstanceOf[LongFloatVector]

                    def writeScore(msgs: Vector): Unit =
                      scoreOutMsgs.update(msgs)

                    def initDegree(msgs: Vector): Unit =
                      degreeMsgs.update(msgs)

                    def readDegree(nodes: Array[Long]): LongLongVector =
                      degreeMsgs.pull(nodes).asInstanceOf[LongLongVector]

                    def initLabel(msgs: Vector): Unit =
                      labelInMsgs.update(msgs)

                    def writeLabel(msgs: Vector): Unit =
                      labelOutMsgs.update(msgs)

                    def readLabel(nodes: Array[Long]): LongLongVector =
                      labelInMsgs.pull(nodes).asInstanceOf[LongLongVector]

                    def numMsgs(): Long =
                      VectorUtils.nnz(scoreInMsgs)

                    def resetMsgs(): Unit = {
                      val tempScore = scoreInMsgs
                      scoreInMsgs = scoreOutMsgs
                      scoreOutMsgs = tempScore
                      scoreOutMsgs.reset

                      val tempLabel = labelInMsgs
                      labelInMsgs = labelOutMsgs
                      labelOutMsgs = tempLabel
                      labelOutMsgs.reset
                    }

                  }

object HANPPSModel {

  def fromMinMax(minId: Long, maxId: Long, psNumPartition: Int,
                 useBalancePartition: Boolean): HANPPSModel = {
    val scoreContext = new ModelContext(psNumPartition, minId, maxId, -1,
      "hanpScore", SparkContext.getOrCreate().hadoopConfiguration)
    val degreeContext = new ModelContext(psNumPartition, minId, maxId, -1,
      "hanpDegree", SparkContext.getOrCreate().hadoopConfiguration)
    val scoreMatrix = ModelContextUtils.createMatrixContext(scoreContext,
      RowType.T_FLOAT_SPARSE_LONGKEY, 2)
    val degreeMatrix = ModelContextUtils.createMatrixContext(degreeContext,
      RowType.T_LONG_SPARSE_LONGKEY, 3)

    PSAgentContext.get().getMasterClient.createMatrix(scoreMatrix, 10000L)
    PSAgentContext.get().getMasterClient.createMatrix(degreeMatrix, 10000L)

    val scoreMatrixId = PSAgentContext.get().getMasterClient
      .getMatrix("hanpScore").getId
    val degreeMatrixId = PSAgentContext.get().getMasterClient
      .getMatrix("hanpDegree").getId
    new HANPPSModel(new PSVectorImpl(scoreMatrixId, 0, maxId, scoreMatrix.getRowType),
      new PSVectorImpl(scoreMatrixId, 1, maxId, scoreMatrix.getRowType),
      new PSVectorImpl(degreeMatrixId, 0, maxId, degreeMatrix.getRowType),
      new PSVectorImpl(degreeMatrixId, 1, maxId, degreeMatrix.getRowType),
      new PSVectorImpl(degreeMatrixId, 2, maxId, degreeMatrix.getRowType)
    )
  }

}
