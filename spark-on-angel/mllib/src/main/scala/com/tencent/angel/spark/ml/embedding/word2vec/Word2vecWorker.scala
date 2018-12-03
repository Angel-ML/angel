package com.tencent.angel.spark.ml.embedding.word2vec

import java.text.SimpleDateFormat
import java.util.Date

import com.tencent.angel.conf.AngelConf
import com.tencent.angel.ml.matrix.RowType
import com.tencent.angel.ps.storage.matrix.PartitionSourceMap
import com.tencent.angel.spark.context.PSContext
import com.tencent.angel.spark.ml.embedding.CBowModel
import com.tencent.angel.spark.ml.embedding.NEModel.NEDataSet
import com.tencent.angel.spark.ml.embedding.word2vec.Word2VecModel.W2VDataSet
import com.tencent.angel.spark.ml.psf.embedding.bad._
import com.tencent.angel.spark.models.PSMatrix
import it.unimi.dsi.fastutil.ints.Int2IntOpenHashMap
import org.apache.spark.rdd.RDD

import scala.util.Random

class Word2vecWorker(numNode: Int,
                     dimension: Int,
                     model: String,
                     numPart: Int,
                     numNodePerRow: Int = -1,
                     seed: Int) extends Serializable {

  val maxNodePerRow: Int = 100000

  val matrix = createPSMatrix(numNode, numPart)
  val rand = new Random(seed)

  initialize()

  def sgdForBatch(partitionId: Int,
                  batchId: Int,
                  negative: Int,
                  window: Int,
                  alpha: Float,
                  batch: NEDataSet): (Double, Long, Array[Long]) = {

    var (start, end) = (0L, 0L)
    val seed = 2017

    // calculate index
    start = System.currentTimeMillis()
    val model = new CBowModel(window, negative, alpha, numNode, dimension)
    val sentences = batch.asInstanceOf[W2VDataSet].sentences
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
    index.defaultReturnValue(-1)
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

//    val batchSize = batch.asInstanceOf[W2VDataSet].sentences.map(_.length).sum
//    println(s"${loss._1/loss._2} learnRate=$alpha length=${loss._2} batchSize=$batchSize")
    (loss._1, loss._2.toLong, Array(calcuIndexTime, pullTime, cbowTime, pushTime))
  }

  def sgdForPartition(partitionId: Int,
                      iterator: Iterator[NEDataSet],
                      window: Int,
                      negative: Int,
                      alpha: Float): Iterator[(Double, Long, Array[Long])] = {
    PSContext.instance()
    val r = iterator.zipWithIndex.map(batch => sgdForBatch(partitionId, batch._2,
      window, negative, alpha, batch._1))
      .reduce { (f1, f2) =>
        (f1._1 + f2._1,
          f1._2 + f2._2,
          f1._3.zip(f2._3).map(f => (f._1 + f._2)))}
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
      val data = trainBatches.next()
      val middle = data.mapPartitionsWithIndex((partitionId, iterator) =>
        sgdForPartition(partitionId, iterator, window, negative, learningRate),
        true).reduce { case (f1, f2) =>
        (f1._1 + f2._1,
          f1._2 + f2._2,
          f1._3.zip(f2._3).map(f => (f._1 + f._2)))}
      val loss = middle._1 / middle._2.toDouble
      val array = middle._3
      logTime(s"epoch=$epoch " +
        f"loss=$loss%2.4f " +
        s"calcuIndexTime=${array(0)} " +
        s"pullTime=${array(1)} " +
        s"cbowTime=${array(2)} " +
        s"pushTime=${array(3)} " +
        s"total=${middle._2} " +
        s"lossSum=${middle._1}")
    }
  }

  private def createPSMatrix(numNode: Int,
                             numPart: Int): PSMatrix = {
    require(numNodePerRow < maxNodePerRow,
      s"size exceed, $numNodePerRow * $maxNodePerRow")

    val rowCapacity = if (numNodePerRow > 0) numNodePerRow else Int.MaxValue / dimension
    val numCol = rowCapacity * dimension * 2
    val numRow = (numNode) / rowCapacity + 1

    val rowsInBlock = numRow / numPart + 1
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

  def logTime(msg: String): Unit = {
    val time = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date)
    println(s"[$time] $msg")
  }

}
