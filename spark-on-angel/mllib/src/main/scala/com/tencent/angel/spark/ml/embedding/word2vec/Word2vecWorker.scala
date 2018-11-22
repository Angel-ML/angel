package com.tencent.angel.spark.ml.embedding.word2vec

import com.tencent.angel.conf.AngelConf
import com.tencent.angel.ml.matrix.RowType
import com.tencent.angel.ps.storage.matrix.PartitionSourceArray
import com.tencent.angel.spark.ml.embedding.NEModel.{NEDataSet, logTime}
import com.tencent.angel.spark.models.PSMatrix
import org.apache.spark.rdd.RDD

class Word2vecWorker(numNode: Int,
                     dimension: Int,
                     model: String,
                     numPart: Int,
                     numNodePerRow: Int = -1,
                     maxNodePerRow: Int = 100000) {

  val matrix = createPSMatrix(numNode, numPart)


  def sgdForPartition(partitionId: Int,
                      iterator: Iterator[NEDataSet],
                      negative: Int,
                      alpha: Float): Iterator[Double] = {
    Iterator.single(0.0)
  }

  def train(trainBatches: Iterator[RDD[NEDataSet]],
            negative: Int,
            numEpoch: Int,
            learningRate: Float,
            path: String,
            checkpointInterval: Int = 10): Unit = {
    for (epoch <- 1 to numEpoch) {

    }
  }

  private def createPSMatrix(numNode: Int,
                             numPart: Int): PSMatrix = {
    require(numNodePerRow < maxNodePerRow,
      s"size exceed, $numNodePerRow * $maxNodePerRow")

    val rowCapacity = if (numNodePerRow > 0) numNodePerRow else Int.MaxValue / dimension
    val numCol = rowCapacity * dimension
    val numRow = (numNode) / rowCapacity + 1

    val rowsInBlock = numRow / numPart
    val colsInBlock = numCol
    logTime(s"matrix meta:\n" +
      s"colNum: $numCol\n" +
      s"rowNum: $numRow\n" +
      s"colsInBlock: $colsInBlock\n" +
      s"rowsInBlock: $rowsInBlock\n" +
      s"numNodesPerRow: $rowCapacity\n"
    )
    val matrix = PSMatrix.dense(numRow, numCol, rowsInBlock, colsInBlock, RowType.T_FLOAT_DENSE,
      Map(AngelConf.ANGEL_PS_PARTITION_SOURCE_CLASS -> classOf[PartitionSourceArray].getName))
    logTime(s"create a dense float matrix")
    matrix
  }

}
