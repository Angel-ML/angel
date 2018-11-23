package com.tencent.angel.spark.ml.embedding.word2vec

import com.tencent.angel.conf.AngelConf
import com.tencent.angel.ml.matrix.RowType
import com.tencent.angel.ps.storage.matrix.PartitionSourceMap
import com.tencent.angel.spark.context.PSContext
import com.tencent.angel.spark.ml.embedding.CBowModel
import com.tencent.angel.spark.ml.embedding.NEModel.{NEDataSet, logTime}
import com.tencent.angel.spark.ml.embedding.word2vec.Word2VecModel.W2VDataSet
import com.tencent.angel.spark.ml.psf.embedding.bad._
import com.tencent.angel.spark.models.PSMatrix
import it.unimi.dsi.fastutil.ints.Int2IntOpenHashMap
import org.apache.spark.rdd.RDD

class Word2vecWorker(numNode: Int,
                     dimension: Int,
                     model: String,
                     numPart: Int,
                     numNodePerRow: Int = -1,
                     maxNodePerRow: Int = 100000) extends Serializable {

  val matrix = createPSMatrix(numNode, numPart)

  initialize()

  def sgdForBatch(partitionId: Int,
                  batchId: Int,
                  negative: Int,
                  window: Int,
                  alpha: Float,
                  batch: NEDataSet): (Double, Array[Long]) = {

    var (start, end) = (0L, 0L)


    // calculate index
    start = System.currentTimeMillis()
    val model = new CBowModel(window, negative, alpha, numNode, dimension)
    val sentences = batch.asInstanceOf[W2VDataSet].sentences
    val seed = System.currentTimeMillis()
    val indices = model.indicesForCbow(sentences, seed)
    end = System.currentTimeMillis()
    val calcuIndexTime = end - start

    // pull
    start = System.currentTimeMillis()
    val result  = matrix.psfGet(new W2VPull(
      new W2VPullParam(matrix.id,
        indices,
        numNodePerRow,
        dimension)))
      .asInstanceOf[W2VPullResult]
    end = System.currentTimeMillis()
    val pullTime = end - start

    val index = new Int2IntOpenHashMap()
    for (i <- 0 until indices.length) index.put(indices(i), i)
    val deltas = new Array[Float](result.layers.length)

    // cbow
    start = System.currentTimeMillis()
    val loss = model.cbow(sentences, seed, result.layers, index, deltas)
    end = System.currentTimeMillis()
    val cbowTime = end - start

    // push
    start = System.currentTimeMillis()
    matrix.psfUpdate(new W2VPush(
      new W2VPushParam(matrix.id,
        indices, deltas, numNodePerRow, dimension)))
        .get()
    end = System.currentTimeMillis()
    val pushTime = end - start
    (loss, Array(calcuIndexTime, pullTime, cbowTime, pullTime))
  }

  def sgdForPartition(partitionId: Int,
                      iterator: Iterator[NEDataSet],
                      window: Int,
                      negative: Int,
                      alpha: Float): Iterator[(Double, Array[Long])] = {
    PSContext.instance()
    val r = iterator.zipWithIndex.map(batch => sgdForBatch(partitionId, batch._2,
      window, negative, alpha, batch._1))
      .reduce { (f1, f2) =>
        (f1._1 + f2._1,
          f1._2.zip(f2._2).map(f => (f._1 + f._2)))}
    Iterator.single(r)
  }

  def train(trainBatches: Iterator[RDD[NEDataSet]],
            negative: Int,
            numEpoch: Int,
            learningRate: Float,
            window: Int,
            path: String,
            checkpointInterval: Int = 10): Unit = {
    for (epoch <- 1 to numEpoch) {
      logTime(s"start epoch $epoch")
      val data = trainBatches.next()
      val middle = data.mapPartitionsWithIndex((partitionId, iterator) =>
        sgdForPartition(partitionId, iterator, window, negative, learningRate),
        true).reduce { case (f1, f2) =>
        (f1._1 + f2._1,
          f1._2.zip(f2._2).map(f => (f._1 + f._2)))}
      val loss = middle._1
      val array = middle._2
      logTime(s"epoch=$epoch " +
        s"loss=$loss " +
        s"calcuIndexTime=${array(0)} " +
        s"pullTime=${array(1)} " +
        s"cbowTime=${array(2)} " +
        s"pushTime=${array(3)}")
    }
  }

  private def createPSMatrix(numNode: Int,
                             numPart: Int): PSMatrix = {
    require(numNodePerRow < maxNodePerRow,
      s"size exceed, $numNodePerRow * $maxNodePerRow")

    val rowCapacity = if (numNodePerRow > 0) numNodePerRow else Int.MaxValue / dimension
    val numCol = rowCapacity * dimension * 2
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
      Map(AngelConf.ANGEL_PS_PARTITION_SOURCE_CLASS -> classOf[PartitionSourceMap].getName))
    logTime(s"create a dense float matrix")
    matrix
  }

  private def initialize(): Unit = {
    matrix.psfUpdate(new W2VRandom(new W2VRandomParam(matrix.id, dimension))).get()
  }

}
