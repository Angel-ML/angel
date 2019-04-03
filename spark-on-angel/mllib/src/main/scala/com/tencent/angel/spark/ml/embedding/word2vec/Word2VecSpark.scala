package com.tencent.angel.spark.ml.embedding.word2vec

import java.text.SimpleDateFormat
import java.util.Date

import com.tencent.angel.conf.AngelConf
import com.tencent.angel.ml.matrix.RowType
import com.tencent.angel.spark.context.PSContext
import com.tencent.angel.spark.ml.embedding.{CBowModel, EmbeddingConf, SGNSModel}
import com.tencent.angel.spark.ml.embedding.NEModel.NEDataSet
import com.tencent.angel.spark.ml.embedding.word2vec.Word2VecModel.W2VDataSet
import com.tencent.angel.spark.ml.psf.embedding.bad._
import com.tencent.angel.spark.models.PSMatrix
import it.unimi.dsi.fastutil.ints.Int2IntOpenHashMap
import org.apache.hadoop.io.{NullWritable, Text}
import org.apache.hadoop.mapred.TextOutputFormat
import org.apache.spark.rdd.RDD

import scala.reflect.ClassTag

class Word2VecSpark(val numNode: Int,
                     val params: Map[String, String]) extends Serializable {

  val dimension = params.getOrElse(EmbeddingConf.EMBEDDINGDIM, "10").toInt
  val modelName = params.getOrElse(EmbeddingConf.MODELTYPE, "sgns")
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
        // TODO: checkpoint (dump to parameter servers, rather than collect to spark workers,
        //  i.e., no re-ordering for the embedding vectors)
        // checkpoint()
      }
    }
  }

  /**
    * collect embedding vectors from parameters servers and save it to hdfs via spark executors.
    * The index2Word is on a RDD from @com.tencent.angel.spark.ml.feature.Features.corpusStringToInt
    * @param index2word: wordIndex --> the wordText
    */
  def saveModel(index2word: RDD[(Int, String)]): Unit = {
    val nullWritableClassTag = implicitly[ClassTag[NullWritable]]
    val textClassTag = implicitly[ClassTag[Text]]
    val word2embedding = index2word.mapPartitions(
      iter => {
        val part_index2word: Array[(Int, String)] = iter.toArray
        val indices: Array[Int] = Array.ofDim(part_index2word.length)
        var cnt = 0
        while(cnt < indices.length){
          indices(cnt) = part_index2word(cnt)._1
          cnt += 1
        }
        val result  = matrix.psfGet(new W2VPull(
          new W2VPullParam(matrix.id,
            indices,
            numNodePerRow,
            dimension)))
          .asInstanceOf[W2VPullResult]

        val word2offset = new Int2IntOpenHashMap() // wordId --> offsetId
        word2offset.defaultReturnValue(-1)
        for (i <- 0 until indices.length) word2offset.put(indices(i), i)

        val embeddings = result.layers // embedding vectors

        // adapted from RDD.saveAsTextFile
        val text = new Text()
        part_index2word.toIterator.map(x =>{
          val word: String = x._2
          val index: Int = x._1
          val offset = word2offset.get(index) * dimension * 2
          val tmpString = embeddings.slice(offset, offset + dimension * 2).mkString(" ")
          text.set(word + " " + tmpString)
          (NullWritable.get(), text)
        })
      })

    RDD.rddToPairRDDFunctions(word2embedding)(nullWritableClassTag, textClassTag, null)
      .saveAsHadoopFile[TextOutputFormat[NullWritable, Text]](outputPath)
  }

  /**
    * initialize the embedding vectors on each worker.
    */
  private def initialize(): Unit = {
    // Here load balance- and communication-aware partition method is needed.

    // initialize word embeddings

    // initialize context embeddings


  }

  def logTime(msg: String): Unit = {
    val time = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date)
    println(s"[$time] $msg")
  }

}
