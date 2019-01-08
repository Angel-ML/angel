package com.tencent.angel.spark.ml.embedding.word2vec

import java.text.SimpleDateFormat
import java.util.Date


import com.tencent.angel.conf.AngelConf
import com.tencent.angel.ml.matrix.RowType
import com.tencent.angel.ps.storage.matrix.PartitionSourceMap
import com.tencent.angel.spark.context.PSContext
import com.tencent.angel.spark.ml.embedding.{CBowModel, EmbeddingConf, SGNSModel}
import com.tencent.angel.spark.ml.embedding.NEModel.NEDataSet
import com.tencent.angel.spark.ml.embedding.word2vec.Word2VecModel.W2VDataSet
import com.tencent.angel.spark.ml.psf.embedding.bad._
import com.tencent.angel.spark.models.PSMatrix
import it.unimi.dsi.fastutil.ints.Int2IntOpenHashMap
import org.apache.spark.rdd.RDD

class Word2vecWorker(val numNode: Int,
                     val params: Map[String, String]) extends Serializable {

  val maxNodePerRow: Int = 100000
  val dimension = params.getOrElse(EmbeddingConf.EMBEDDINGDIM, "10").toInt
  val modelName = params.getOrElse(EmbeddingConf.MODELTYPE, "SGNS")
  val numPart = params.getOrElse(EmbeddingConf.NUMPARTITIONS, "10").toInt
  val numNodePerRow = params.getOrElse(EmbeddingConf.NUMNODEPERROW, "10000").toInt
  val negative = params.getOrElse(EmbeddingConf.NEGATIVESAMPLENUM, "5").toInt
  val window = params.getOrElse(EmbeddingConf.WINDOWSIZE, "5").toInt
  val alpha = params.getOrElse(EmbeddingConf.STEPSIZE, "0.1").toFloat
  val rprEndPrp = params.getOrElse(EmbeddingConf.ROOTEDPAGERANKPRP, "0.5").toFloat
  val numEpoch = params.getOrElse(EmbeddingConf.NUMEPOCH, "10").toInt
  val outputPath = params.getOrElse(EmbeddingConf.OUTPUTPATH, "")
  val checkpointInterval = params.getOrElse(EmbeddingConf.CHECKPOINTINTERVAL, numEpoch.toString).toInt
  // by default, we don't do checkpoint
  val samplerName = params.getOrElse(EmbeddingConf.SAMPLERNAME, "rootedPageRank")

  val matrix = createPSMatrix(numNode, numPart)

  val model = modelName match {
    case "cbow" => new CBowModel(negative, alpha, numNode, dimension, window)
    case "sgns" => new SGNSModel(negative, alpha, numNode, dimension, samplerName, rprEndPrp)
    case _ => new SGNSModel(negative, alpha, numNode, dimension, samplerName, rprEndPrp) // default is SGNS
  }

  initialize()

  /**
    * @param batch
    * @param epochId use epochId as seed for random number generator
    * @return (sum_loss, loss_cnt, evaluation metrics)
    */
  def sgdForBatch(batch: NEDataSet,
                  epochId: Int): (Double, Long, Array[Long]) = {

    var (start, end) = (0L, 0L)
    val seed = epochId

    // calculate index
    start = System.currentTimeMillis()
    val sentences = batch.asInstanceOf[W2VDataSet].sentences
    val indices =  model.buildIndices(sentences, seed)
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

    // train
    start = System.currentTimeMillis()
    val loss = model.train(sentences, seed, result.layers, index, deltas)
    end = System.currentTimeMillis()
    val trainTime = end - start

    // push
    start = System.currentTimeMillis()
    matrix.psfUpdate(new W2VPush(
      new W2VPushParam(matrix.id,
        indices, deltas, numNodePerRow, dimension)))
        .get()
    end = System.currentTimeMillis()
    val pushTime = end - start

    (loss._1, loss._2.toLong, Array(calcuIndexTime, pullTime, trainTime, pushTime))
  }

  def sgdForPartition(iterator: Iterator[NEDataSet],
                      epochId: Int): Iterator[(Double, Long, Array[Long])] = {
    PSContext.instance()
    val r = iterator.map(batch => sgdForBatch(batch, epochId))
      .reduce { (f1, f2) =>
        (f1._1 + f2._1,
          f1._2 + f2._2,
          f1._3.zip(f2._3).map(f => (f._1 + f._2)))}
    Iterator.single(r)
  }

  def train(trainBatches: Iterator[RDD[NEDataSet]]) : Unit = {

    for (epochId <- 1 to numEpoch) {
      val data = trainBatches.next()
      val middle = data.mapPartitions(iterator =>
        sgdForPartition(iterator, epochId), true).reduce { case (f1, f2) =>
          (f1._1 + f2._1,
          f1._2 + f2._2,
          f1._3.zip(f2._3).map(f => (f._1 + f._2))
          )}
      val loss = middle._1 / middle._2.toDouble
      val array = middle._3
      logTime(s"epoch=$epochId " +
        f"loss=$loss%2.4f " +
        s"calcuIndexTime=${array(0)} " +
        s"pullTime=${array(1)} " +
        s"trainTime=${array(2)} " +
        s"pushTime=${array(3)} " +
        s"total=${middle._2} " +
        s"lossSum=${middle._1}")

      if(epochId % checkpointInterval == 0){
        // TODO: checkpoint
        // save()
      }
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
