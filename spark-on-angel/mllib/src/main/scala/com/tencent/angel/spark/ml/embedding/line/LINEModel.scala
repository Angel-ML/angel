/*
 * Tencent is pleased to support the open source community by making Angel available.
 *
 * Copyright (C) 2017-2018 THL A29 Limited, a Tencent company. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in
 * compliance with the License. You may obtain a copy of the License at
 *
 * https://opensource.org/licenses/Apache-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 *
 */

package com.tencent.angel.spark.ml.embedding.line

import java.util.{Random, HashMap => JHashMap, HashSet => JHashSet}

import com.tencent.angel.spark.context.{AngelPSContext, PSContext}
import com.tencent.angel.spark.ml.embedding.FastSigmoid
import com.tencent.angel.spark.ml.embedding.NEModel.NEDataSet
import com.tencent.angel.spark.ml.embedding.line.LINEModel.{LINEDataSet, buildDataBatches}
import com.tencent.angel.spark.ml.util.LogUtils
import it.unimi.dsi.fastutil.ints.{Int2IntOpenHashMap, Int2ObjectOpenHashMap}
import org.apache.hadoop.fs.Path
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.types.LongType
import org.apache.spark.storage.StorageLevel

import scala.collection.mutable.ArrayBuffer

/**
  * LINE mode used to handle no-weight graph
  *
  * @param dataset            training data set
  * @param embeddingDim       node embedding dimension
  * @param negativeNum        negative sample number
  * @param stepSize           learning rate
  * @param order              order
  * @param psPartNum          partition number of the model on PS
  * @param batchSize          size of mini-batch
  * @param epochNum           epoch number
  * @param dataPartNum        data partition number
  * @param srcNodeIdCol       src node column name
  * @param dstNodeIdCol       dst node column name
  * @param needMapping        remapping the node id to int or not
  * @param needSumSampling    need sub-sample
  * @param output             model and remapping result output path
  * @param checkpointInterval checkpoint interval
  * @param modelSaveInterval  mode save interval
  */
class LINEModel(dataset: Dataset[_], embeddingDim: Int, negativeNum: Int, stepSize: Float, order: Int,
                psPartNum: Int, batchSize: Int, epochNum: Int, dataPartNum: Int, srcNodeIdCol: String,
                dstNodeIdCol: String, needMapping: Boolean, needSumSampling: Boolean,
                output: String, checkpointInterval: Int, modelSaveInterval: Int, saveMeta: Boolean, oldModelPath: String) extends Serializable {

  /**
    * Model on PS
    */
  @volatile var psModel: LINEPSModel = _

  def train(): Unit = {
    // Original edges
    var edges:RDD[(String, String)] = null
    if(dataset.schema.fields(0).dataType == LongType && dataset.schema.fields(1).dataType == LongType) {
      edges = dataset.select(srcNodeIdCol, dstNodeIdCol).rdd
        .map(row => (row.getLong(0).toString, row.getLong(1).toString))
        .filter(f => f._1 != f._2)
    } else {
      edges = dataset.select(srcNodeIdCol, dstNodeIdCol).rdd
        .map(row => (row.getString(0), row.getString(1)))
        .filter(f => f._1 != f._2)
    }

    edges.persist(StorageLevel.DISK_ONLY)

    // Remapping the node id to int if need
    val newEdges = if (needMapping) remapping(edges) else edges.map(edge => (edge._1.toInt, edge._2.toInt))
    newEdges.persist(StorageLevel.DISK_ONLY)

    // Unpersist the original edges
    edges.unpersist()

    // Get node id range and the number of total edges
    val (minId, maxId, numEdges) = newEdges.mapPartitions(summarizeApplyOp)
      .reduce(summarizeReduceOp)

    LogUtils.logTime(s"minId=$minId, maxId=$maxId, numEdges=$numEdges, embedding dim=$embeddingDim")

    // Start PS and init the model
    LogUtils.logTime("start to run ps")
    PSContext.getOrCreate(SparkContext.getOrCreate())

    // Create matrix and init PS model
    LogUtils.logTime("Create matrices in PS")
    initPSModel(maxId, maxId, oldModelPath)

    // Get mini-batch data set
    val trainBatches = buildDataBatches(newEdges, batchSize)

    val startTs = System.currentTimeMillis()
    // Before training, checkpoint the model
    getPSModel.checkpointEmbeddingMatrix(0)
    LogUtils.logTime(s"Write checkpoint use time=${System.currentTimeMillis() - startTs}")

    for (epoch <- 1 to epochNum) {
      val alpha = stepSize
      val data = trainBatches.next()
      val numPartitions = data.getNumPartitions
      val middle = data.mapPartitionsWithIndex((partitionId, iterator) =>
        sgdForPartition(partitionId, iterator, numPartitions, negativeNum, alpha),
        preservesPartitioning = true
      ).collect()
      val loss = middle.map(f => f._1).sum / middle.map(_._2).sum.toFloat
      val array = new Array[Long](6)
      middle.foreach(f => f._3.zipWithIndex.foreach(t => array(t._2) += t._1))

      LogUtils.logTime(s"epoch=$epoch " +
        f"loss=$loss%2.4f " +
        s"sampleTime=${array(0)} getEmbeddingTime=${array(1)} " +
        s"dotTime=${array(2)} gradientTime=${array(3)} calUpdateTime=${array(4)} pushTime=${array(5)} " +
        s"total=${middle.map(_._2).sum.toFloat} lossSum=${middle.map(_._1).sum} ")

      // Write the checkpoint or model result if need
      checkpointAndSaveIfNeed(epoch)
    }
  }

  def remapping(data: RDD[(String, String)]): RDD[(Int, Int)] = {
    // All distinct node ids
    val strings = data.map(f => Array(f._1, f._2)).flatMap(f => f)
      .map(t => (t, 1)).reduceByKey(_ + _).map(f => f._1)

    // RDD[(String, Int)]: node id to node index
    val stringsWithIndex = strings.zipWithIndex().cache()

    def buildRoutingTable(index: Int, iterator: Iterator[(String, String)]): Iterator[(String, Int)] = {
      val set = new JHashSet[String]()
      while (iterator.hasNext) {
        val line = iterator.next()
        set.add(line._1)
        set.add(line._2)
      }

      val it = set.iterator()
      val result = new ArrayBuffer[(String, Int)]()
      while (it.hasNext) {
        result.append((it.next(), index))
      }

      result.iterator
    }

    // RDD[(String, Int)]: node id to data partition id map
    val routingTable = data.mapPartitionsWithIndex((partId, iterator) =>
      buildRoutingTable(partId, iterator),
      true)

    // RDD[Int, Iterator(String, Int)]: data partition id to (node id to node index) arrays
    val partIndex = routingTable.join(stringsWithIndex).map { case (str, (partId, index)) =>
      (partId, (str, index))
    }.groupByKey(data.getNumPartitions)

    def attachPartitionId(index: Int, iterator: Iterator[(String, String)]): Iterator[(Int, Array[(String, String)])] = {
      Iterator.single((index, iterator.toArray))
    }

    // RDD[Int, Array[(String, String)]](data partition id to edges) join RDD[Int, Iterator(String, Int)](data partition id to (node id to node index))
    // Transfer the original edges(node id) to new edges(node index)
    val ints: RDD[(Int, Int)] = data.mapPartitionsWithIndex((partId, iterator) =>
      attachPartitionId(partId, iterator),
      true).join(partIndex).map { case (_, (sentences, mapping)) =>
      val map = new JHashMap[String, Long](mapping.size)
      for ((string, index) <- mapping) {
        map.put(string, index)
      }

      sentences.map(s => (map.get(s._1).toInt, map.get(s._2).toInt))
    }.flatMap(f => f)


    // Write the node id to node index map to hdfs
    saveRemappingResult(stringsWithIndex)

    ints
  }

  /**
    * Save the node id mapping result to hdfs
    *
    * @param stringsWithIndex RDD[(String, Int)]: (node id, node index)
    */
  def saveRemappingResult(stringsWithIndex: RDD[(String, Long)]): Unit = {
    // Remove the remapping file if exist
    try {
      val mappingPath = new Path(output, "mapping")
      // Build hadoop conf
      val conf = AngelPSContext.convertToHadoop(SparkContext.getOrCreate().getConf)
      val fs = mappingPath.getFileSystem(conf)
      // Remove
      val ret = fs.delete(mappingPath, true)
      if (!ret) {
        LogUtils.logTime(s"Warning: remove meta file failed !!!")
      }

      // Write the mapping result
      stringsWithIndex.map(f => (f._2.toInt, f._1)).map(f => s"${f._1}:${f._2}").saveAsTextFile(mappingPath.toString)
    } catch {
      case e: Throwable => LogUtils.logTime(s"Warning: remove meta file failed !!!")
    }
  }

  /**
    * Write checkpoint or model result if need
    *
    * @param epoch
    */
  def checkpointAndSaveIfNeed(epoch: Int): Unit = {
    var startTs = 0L
    if (epoch % checkpointInterval == 0 && epoch < epochNum) {
      LogUtils.logTime(s"Epoch=$epoch, checkpoint the model")
      startTs = System.currentTimeMillis()
      getPSModel.checkpointEmbeddingMatrix(epoch)
      LogUtils.logTime(s"checkpoint use time=${System.currentTimeMillis() - startTs}")
    }

    if (epoch % modelSaveInterval == 0 && epoch < epochNum) {
      LogUtils.logTime(s"Epoch=$epoch, save the model")
      startTs = System.currentTimeMillis()
      getPSModel.save(output, epoch, saveMeta)
      LogUtils.logTime(s"save use time=${System.currentTimeMillis() - startTs}")
    }
  }

  /**
    * Init the model on PS
    *
    * @param minId min node id
    * @param maxId max node id
    */
  def initPSModel(minId: Int, maxId: Int, oldModelPath: String): Unit = {
    psModel = LINEPSModel.fromMinMax(maxId, maxId + 1, psPartNum, order, embeddingDim, isWeighted = false, oldModelPath)
  }

  def getPSModel: LINEPSModel = psModel

  def sgdForPartition(partitionId: Int,
                      iterator: Iterator[NEDataSet],
                      numPartitions: Int,
                      negative: Int,
                      alpha: Float): Iterator[(Float, Long, Array[Long])] = {

    PSContext.instance()
    val rand = new Random(this.hashCode() + 31 * partitionId)
    iterator.zipWithIndex.map { case (batch, index) =>
      sgdForBatch(partitionId, batch.asInstanceOf[LINEDataSet], index, alpha, rand.nextInt())
    }
  }

  /**
    * Handle a mini-batch edges
    *
    * @param partitionId data partition id
    * @param batch       mini-batch edges
    * @param batchId     mini-batch index
    * @param alpha       learning rate in this epoch
    * @param seed        random seed
    * @return
    */
  def sgdForBatch(partitionId: Int,
                  batch: LINEDataSet,
                  batchId: Int,
                  alpha: Float, seed: Int): (Float, Long, Array[Long]) = {

    if (batch.src.length == 0) {
      LogUtils.logTime("batch size is 0, just return")
      return (0.0f, 1, Array(0, 0, 0, 0, 0))
    }

    var start = 0L

    start = System.currentTimeMillis()
    val srcNodes = batch.src
    val destNodes = batch.dst
    val negativeSamples = getPSModel.negativeSample(srcNodes, destNodes, negativeNum, seed)
    val sampleTime = System.currentTimeMillis() - start

    // Get node embedding from PS
    start = System.currentTimeMillis()
    val getResult = getPSModel.getEmbedding(srcNodes, destNodes, negativeSamples, negativeNum, order)
    val srcFeats: Int2ObjectOpenHashMap[Array[Float]] = getResult._1
    val targetFeats: Int2ObjectOpenHashMap[Array[Float]] = getResult._2

    val getEmbeddingTime = System.currentTimeMillis() - start

    // Get dot values
    start = System.currentTimeMillis()
    val dots = dot(srcNodes, destNodes, negativeSamples, srcFeats, targetFeats)
    val dotTime = System.currentTimeMillis() - start

    // gradient
    start = System.currentTimeMillis()
    val loss = doGrad(dots, alpha)
    val gradientTime = System.currentTimeMillis() - start

    // Get the embedding updates
    start = System.currentTimeMillis()
    val (inputUpdates, outputUpdates) = adjust(srcNodes, destNodes, negativeSamples, srcFeats, targetFeats, dots)
    val calUpdateTime = System.currentTimeMillis() - start

    // Push the updates to PS
    start = System.currentTimeMillis()
    getPSModel.adjust(inputUpdates, outputUpdates, order)
    val pushTime = System.currentTimeMillis() - start

    if (batchId % 10 == 0) {
      LogUtils.logTime(s"loss=$loss sampleTime=$sampleTime getEmbeddingTime=$getEmbeddingTime " +
        s"dotTime=$dotTime gradientTime=$gradientTime calUpdateTime=$calUpdateTime " +
        s"pushTime=$pushTime")
    }

    (loss, dots.length.toLong, Array(sampleTime, getEmbeddingTime, dotTime, gradientTime, calUpdateTime, pushTime))
  }


  /**
    * Get dots values of node embedding vectors
    *
    * @param srcNodes        src nodes
    * @param destNodes       dst nodes
    * @param negativeSamples negative samples
    * @param srcFeats        src node to embedding vector map
    * @param targetFeats     dst and negative samples to embedding vector map
    * @return
    */
  def dot(srcNodes: Array[Int], destNodes: Array[Int], negativeSamples: Array[Array[Int]],
          srcFeats: Int2ObjectOpenHashMap[Array[Float]], targetFeats: Int2ObjectOpenHashMap[Array[Float]]): Array[Float] = {
    val dots: Array[Float] = new Array[Float]((1 + negativeNum) * srcNodes.length)
    if (order == 1) {
      var docIndex = 0
      for (i <- srcNodes.indices) {
        val srcVec = srcFeats.get(srcNodes(i))
        // Get dot value for (src, dst)
        dots(docIndex) = arraysDot(srcVec, srcFeats.get(destNodes(i)))
        docIndex += 1

        // Get dot value for (src, negative sample)
        for (j <- 0 until negativeNum) {
          dots(docIndex) = arraysDot(srcVec, srcFeats.get(negativeSamples(i)(j)))
          docIndex += 1
        }
      }
      dots
    } else {
      var docIndex = 0
      for (i <- srcNodes.indices) {
        val srcVec = srcFeats.get(srcNodes(i))
        // Get dot value for (src, dst)
        dots(docIndex) = arraysDot(srcVec, targetFeats.get(destNodes(i)))
        docIndex += 1

        // Get dot value for (src, negative sample)
        for (j <- 0 until negativeNum) {
          dots(docIndex) = arraysDot(srcVec, targetFeats.get(negativeSamples(i)(j)))
          docIndex += 1
        }
      }
      dots
    }
  }

  def arraysDot(x: Array[Float], y: Array[Float]): Float = {
    var dotValue = 0.0f
    x.indices.foreach(i => dotValue += x(i) * y(i))
    dotValue
  }

  def axpy(y: Array[Float], x: Array[Float], a: Float) = {
    x.indices.foreach(i => y(i) += a * x(i))
  }

  def div(x: Array[Float], f: Float): Unit = {
    x.indices.foreach(i => x(i) = x(i) / f)
  }

  /**
    * Get the embedding vector updates for all nodes(src nodes, dst nodes and negative samples)
    *
    * @param srcNodes        src nodes
    * @param destNodes       dst nodes
    * @param negativeSamples negative samples
    * @param srcFeats        src node to embedding vector map
    * @param targetFeats     dst and negative samples to embedding vector map
    * @param dots            dot values
    * @return
    */
  def adjust(srcNodes: Array[Int], destNodes: Array[Int], negativeSamples: Array[Array[Int]],
             srcFeats: Int2ObjectOpenHashMap[Array[Float]], targetFeats: Int2ObjectOpenHashMap[Array[Float]],
             dots: Array[Float]): (Int2ObjectOpenHashMap[Array[Float]], Int2ObjectOpenHashMap[Array[Float]]) = {
    if (order == 1) {
      val inputUpdateCounter = new Int2IntOpenHashMap(srcFeats.size())
      val inputUpdates = new Int2ObjectOpenHashMap[Array[Float]](srcFeats.size())

      var docIndex = 0
      for (i <- srcNodes.indices) {
        // Src node grad
        val neule = new Array[Float](embeddingDim)

        // Accumulate dst node embedding to neule
        val dstEmbedding = srcFeats.get(destNodes(i))
        var g = dots(docIndex)
        axpy(neule, dstEmbedding, g)

        // Use src node embedding to update dst node embedding
        val srcEmbedding = srcFeats.get(srcNodes(i))
        merge(inputUpdateCounter, inputUpdates, destNodes(i), g, srcEmbedding)

        docIndex += 1

        // Use src node embedding to update negative sample node embedding; Accumulate negative sample node embedding to neule
        for (j <- 0 until negativeNum) {
          val negSampleEmbedding = srcFeats.get(negativeSamples(i)(j))
          g = dots(docIndex)

          // Accumulate negative sample node embedding to neule
          axpy(neule, negSampleEmbedding, g)

          // Use src node embedding to update negative sample node embedding
          merge(inputUpdateCounter, inputUpdates, negativeSamples(i)(j), g, srcEmbedding)
          docIndex += 1
        }

        // Use accumulation to update src node embedding, grad = 1
        merge(inputUpdateCounter, inputUpdates, srcNodes(i), 1, neule)
      }

      val iter = inputUpdateCounter.entrySet().iterator()
      while (iter.hasNext) {
        val entry = iter.next()
        div(inputUpdates.get(entry.getKey.toInt), entry.getValue.toFloat)
      }

      (inputUpdates, null)
    } else {
      val inputUpdateCounter = new Int2IntOpenHashMap(srcFeats.size())
      val inputUpdates = new Int2ObjectOpenHashMap[Array[Float]](srcFeats.size())

      val outputUpdateCounter = new Int2IntOpenHashMap(targetFeats.size())
      val outputUpdates = new Int2ObjectOpenHashMap[Array[Float]](targetFeats.size())

      var docIndex = 0
      for (i <- srcNodes.indices) {
        // Src node grad
        val neule = new Array[Float](embeddingDim)

        // Accumulate dst node embedding to neule
        val dstEmbedding = targetFeats.get(destNodes(i))
        var g = dots(docIndex)
        axpy(neule, dstEmbedding, g)

        // Use src node embedding to update dst node embedding
        val srcEmbedding = srcFeats.get(srcNodes(i))
        merge(outputUpdateCounter, outputUpdates, destNodes(i), g, srcEmbedding)

        docIndex += 1

        // Use src node embedding to update negative sample node embedding; Accumulate negative sample node embedding to neule
        for (j <- 0 until negativeNum) {
          val negSampleEmbedding = targetFeats.get(negativeSamples(i)(j))
          g = dots(docIndex)

          // Accumulate negative sample node embedding to neule
          axpy(neule, negSampleEmbedding, g)

          // Use src node embedding to update negative sample node embedding
          merge(outputUpdateCounter, outputUpdates, negativeSamples(i)(j), g, srcEmbedding)
          docIndex += 1
        }

        // Use accumulation to update src node embedding, grad = 1
        merge(inputUpdateCounter, inputUpdates, srcNodes(i), 1, neule)
      }

      var iter = inputUpdateCounter.int2IntEntrySet().fastIterator()
      while (iter.hasNext) {
        val entry = iter.next()
        div(inputUpdates.get(entry.getIntKey), entry.getIntValue.toFloat)
      }

      iter = outputUpdateCounter.int2IntEntrySet().fastIterator()
      while (iter.hasNext) {
        val entry = iter.next()
        div(outputUpdates.get(entry.getIntKey), entry.getIntValue.toFloat)
      }

      (inputUpdates, outputUpdates)
    }
  }

  def merge(inputUpdateCounter: Int2IntOpenHashMap, inputUpdates: Int2ObjectOpenHashMap[Array[Float]],
            nodeId: Int, g: Float, update: Array[Float]): Int = {
    var grads: Array[Float] = inputUpdates.get(nodeId)
    if (grads == null) {
      grads = new Array[Float](embeddingDim)
      inputUpdates.put(nodeId, grads)
      inputUpdateCounter.put(nodeId, 0)
    }

    //grads.iaxpy(update, g)
    axpy(grads, update, g)
    inputUpdateCounter.addTo(nodeId, 1)
  }

  def doGrad(dots: Array[Float], alpha: Float): Float = {
    var loss = 0.0f
    for (i <- dots.indices) {
      val prob = FastSigmoid.sigmoid(dots(i))
      if (i % (negativeNum + 1) == 0) {
        dots(i) = alpha * (1 - prob)
        loss -= FastSigmoid.log(prob)
      } else {
        dots(i) = -alpha * FastSigmoid.sigmoid(dots(i))
        loss -= FastSigmoid.log(1 - prob)
      }
    }
    loss
  }

  def summarizeReduceOp(t1: (Int, Int, Long),
                        t2: (Int, Int, Long)): (Int, Int, Long) =
    (math.min(t1._1, t2._1), math.max(t1._2, t2._2), t1._3 + t2._3)

  def summarizeApplyOp(iterator: Iterator[(Int, Int)]): Iterator[(Int, Int, Long)] = {
    var minId = Int.MaxValue
    var maxId = Int.MinValue
    var numEdges = 0L
    while (iterator.hasNext) {
      val entry = iterator.next()
      val (src, dst) = (entry._1, entry._2)
      minId = math.min(minId, src)
      minId = math.min(minId, dst)
      maxId = math.max(maxId, src)
      maxId = math.max(maxId, dst)
      numEdges += 1
    }

    Iterator.single((minId, maxId, numEdges))
  }

  def save(outPath:String, epochNum:Int, saveMeta: Boolean): Unit = {
    psModel.save(outPath, epochNum, saveMeta)
  }
}

object LINEModel {

  def buildDataBatches(trainSet: RDD[(Int, Int)], batchSize: Int): Iterator[RDD[NEDataSet]] = {
    new Iterator[RDD[NEDataSet]] with Serializable {
      override def hasNext(): Boolean = true

      override def next(): RDD[NEDataSet] = {
        trainSet.mapPartitions { iter =>
          val shuffledIter = scala.util.Random.shuffle(iter)
          asLineBatch(shuffledIter, batchSize)
        }
      }
    }
  }

  def asLineBatch(iter: Iterator[(Int, Int)], batchSize: Int): Iterator[NEDataSet] = {
    val src = new Array[Int](batchSize)
    val dst = new Array[Int](batchSize)
    new Iterator[NEDataSet] {
      override def hasNext: Boolean = iter.hasNext

      override def next(): NEDataSet = {
        var pos = 0
        while (iter.hasNext && pos < batchSize) {
          val (s, d) = iter.next()
          src(pos) = s
          dst(pos) = d
          pos += 1
        }
        if (pos < batchSize) LINEDataSet(src.take(pos), dst.take(pos)) else LINEDataSet(src, dst)
      }
    }
  }

  case class LINEDataSet(src: Array[Int], dst: Array[Int]) extends NEDataSet

}

/**
  * LINE mode used to handle no-weight graph
  *
  * @param dataset            training data set
  * @param embeddingDim       node embedding dimension
  * @param negativeNum        negative sample number
  * @param stepSize           learning rate
  * @param order              order
  * @param psPartNum          partition number of the model on PS
  * @param batchSize          size of mini-batch
  * @param epochNum           epoch number
  * @param dataPartNum        data partition number
  * @param srcNodeIdCol       src node column name
  * @param dstNodeIdCol       dst node column name
  * @param weightCol          weight column name
  * @param needMapping        remapping the node id to int or not
  * @param needSumSampling    need sub-sample
  * @param output             model and remapping result output path
  * @param checkpointInterval checkpoint interval
  * @param modelSaveInterval  mode save interval
  */
class LINEWithWightModel(dataset: Dataset[_], embeddingDim: Int, negativeNum: Int, stepSize: Float, order: Int,
                         psPartNum: Int, batchSize: Int, epochNum: Int, dataPartNum: Int, srcNodeIdCol: String,
                         dstNodeIdCol: String, weightCol: String, needMapping: Boolean, needSumSampling: Boolean,
                         output: String, checkpointInterval: Int, modelSaveInterval: Int, saveMeta: Boolean, oldModelPath: String)
  extends LINEModel(dataset, embeddingDim, negativeNum, stepSize, order, psPartNum, batchSize,
    epochNum, dataPartNum, srcNodeIdCol, dstNodeIdCol, needMapping, needSumSampling, output, checkpointInterval, modelSaveInterval, saveMeta, oldModelPath) {

  override def train(): Unit = {
    // Generate edge table with weight
    var edges:RDD[(String, String, Float)] = null
    if(dataset.schema.fields(0).dataType == LongType && dataset.schema.fields(1).dataType == LongType) {
      edges =
        dataset.select(srcNodeIdCol, dstNodeIdCol, weightCol).rdd
          .map(row => (row.getLong(0).toString, row.getLong(1).toString, row.getFloat(2)))
          .filter(f => f._1 != f._2)
          .filter(f => f._3 != 0)
    } else {
      edges =
        dataset.select(srcNodeIdCol, dstNodeIdCol, weightCol).rdd
          .map(row => (row.getString(0), row.getString(1), row.getFloat(2)))
          .filter(f => f._1 != f._2)
          .filter(f => f._3 != 0)
    }

    edges.persist(StorageLevel.DISK_ONLY)

    // Remapping the node id to int if need
    val newEdges = if (needMapping) remappingWithWeight(edges) else edges.map(edge => (edge._1.toInt, edge._2.toInt, edge._3))
    newEdges.persist(StorageLevel.DISK_ONLY)
    edges.unpersist()

    // Get the node index range and the edges number
    val (minId, maxId, numEdges) = newEdges.map(f => (f._1, f._2)).mapPartitions(summarizeApplyOp)
      .reduce(summarizeReduceOp)

    LogUtils.logTime(s"minId=$minId, maxId=$maxId, numEdges=$numEdges, embedding dim=$embeddingDim")

    // Start PS and init the model
    LogUtils.logTime("start to run ps")
    PSContext.getOrCreate(SparkContext.getOrCreate())

    // Create matrix and init PS model
    LogUtils.logTime("Create matrices in PS")
    initPSModel(maxId, maxId, oldModelPath)

    // Group by the edge use src node id
    LogUtils.logTime("Group by the edge use src node id")
    val graph: RDD[(Int, Iterable[(Int, Float)])] = newEdges.map(sd => (sd._1, (sd._2, sd._3)))
      .groupByKey(dataPartNum).persist(StorageLevel.DISK_ONLY)

    // Init neighbor table and weights
    LogUtils.logTime("Init neighbor table and weights")
    graph.mapPartitions { iter => {
      // Init the neighbor table use many mini-batch to avoid big object
      iter.sliding(batchSize, batchSize).map(pairs => getPSModel.initNeighbors(pairs))
    }
    }.count()

    // Build global alias table on PS
    LogUtils.logTime("Build global alias table")
    getPSModel.initNeighborsOver()

    // Checkpoint the alias table
    getPSModel.checkpointEmbeddingAndAliasTable(0)

    val seed = System.currentTimeMillis().toInt
    for (epoch <- 1 to epochNum) {
      val middle = edges.mapPartitionsWithIndex((partId, iter) => {
        iter.sliding(batchSize, batchSize).zipWithIndex.map(e => {
          sgdForBatch(batchSize, e._2, stepSize, order, seed + partId)
        })
      }).collect()

      val loss = middle.map(f => f._1).sum / middle.map(_._2).sum.toFloat
      val array = new Array[Long](7)
      middle.foreach(f => f._3.zipWithIndex.foreach(t => array(t._2) += t._1))

      LogUtils.logTime(s"epoch=$epoch " +
        f"loss=$loss%2.4f " +
        s"sampleEdgeTime=${array(0)} " +
        s"sampleTime=${array(1)} getEmbeddingTime=${array(2)} " +
        s"dotTime=${array(3)} gradientTime=${array(4)} calUpdateTime=${array(5)} pushTime=${array(6)} " +
        s"total=${middle.map(_._2).sum.toFloat} lossSum=${middle.map(_._1).sum} ")

      checkpointAndSaveIfNeed(epoch)
    }


    dataset.sparkSession.emptyDataFrame
  }

  def sgdForBatch(batchSize: Int,
                  batchId: Int,
                  alpha: Float, order: Int, seed: Int): (Float, Long, Array[Long]) = {

    var start = 0L

    // Sample the edge by weight
    start = System.currentTimeMillis()
    val batch = getPSModel.sampleEdges(batchSize, 1)
    val sampleWeightsTime = System.currentTimeMillis() - start

    start = System.currentTimeMillis()
    val srcNodes = batch._1
    val destNodes = batch._2

    val negativeSamples = getPSModel.negativeSample(srcNodes, destNodes, negativeNum, seed)
    val sampleTime = System.currentTimeMillis() - start

    // Get node embedding from PS
    start = System.currentTimeMillis()
    val getResult = getPSModel.getEmbedding(srcNodes, destNodes, negativeSamples, negativeNum, order)
    val srcFeats: Int2ObjectOpenHashMap[Array[Float]] = getResult._1
    val targetFeats: Int2ObjectOpenHashMap[Array[Float]] = getResult._2
    val getEmbeddingTime = System.currentTimeMillis() - start

    // Get dot values
    start = System.currentTimeMillis()
    val dots = dot(srcNodes, destNodes, negativeSamples, srcFeats, targetFeats)
    val dotTime = System.currentTimeMillis() - start

    // gradient
    start = System.currentTimeMillis()
    val loss = doGrad(dots, alpha)
    val gradientTime = System.currentTimeMillis() - start

    start = System.currentTimeMillis()
    val (inputUpdates, outputUpdates) = adjust(srcNodes, destNodes, negativeSamples, srcFeats, targetFeats, dots)
    val calUpdateTime = System.currentTimeMillis() - start

    start = System.currentTimeMillis()
    getPSModel.adjust(inputUpdates, outputUpdates, order)
    val pushTime = System.currentTimeMillis() - start

    if (batchId % 10 == 0) {
      LogUtils.logTime(s"loss=$loss sampleWeightsTime=$sampleWeightsTime, sampleTime=$sampleTime getEmbeddingTime=$getEmbeddingTime " +
        s"dotTime=$dotTime gradientTime=$gradientTime calUpdateTime=$calUpdateTime " +
        s"pushTime=$pushTime")
    }

    (loss, dots.length.toLong, Array(sampleWeightsTime, sampleTime, getEmbeddingTime, dotTime, gradientTime, calUpdateTime, pushTime))
  }

  override def initPSModel(minId: Int, maxId: Int, oldModelPath: String): Unit = {
    psModel = LINEPSModel.fromMinMax(maxId, maxId + 1, psPartNum, order, embeddingDim, isWeighted = true, oldModelPath)
  }

  override def getPSModel: LINEWithWeightPSModel = super.getPSModel.asInstanceOf[LINEWithWeightPSModel]


  def remappingWithWeight(data: RDD[(String, String, Float)]): RDD[(Int, Int, Float)] = {
    // All distinct node ids
    val strings = data.map(f => Array(f._1, f._2)).flatMap(f => f)
      .map(t => (t, 1)).reduceByKey(_ + _).map(f => f._1)

    // RDD[(String, Int)]: node id to node index
    val stringsWithIndex = strings.zipWithIndex().cache()

    def buildRoutingTable(index: Int, iterator: Iterator[(String, String, Float)]): Iterator[(String, Int)] = {
      val set = new JHashSet[String]()
      while (iterator.hasNext) {
        val line = iterator.next()
        set.add(line._1)
        set.add(line._2)
      }

      val it = set.iterator()
      val result = new ArrayBuffer[(String, Int)]()
      while (it.hasNext) {
        result.append((it.next(), index))
      }

      result.iterator
    }

    // RDD[(String, Int)]: node id to data partition id map
    val routingTable = data.mapPartitionsWithIndex((partId, iterator) =>
      buildRoutingTable(partId, iterator),
      true)

    // RDD[Int, Iterator(String, Int)]: data partition id to (node id to node index) arrays
    val partIndex = routingTable.join(stringsWithIndex).map { case (str, (partId, index)) =>
      (partId, (str, index))
    }.groupByKey(data.getNumPartitions)

    def attachPartitionId(index: Int, iterator: Iterator[(String, String, Float)]): Iterator[(Int, Array[(String, String, Float)])] = {
      Iterator.single((index, iterator.toArray))
    }

    // RDD[Int, Array[(String, String)]](data partition id to edges) join RDD[Int, Iterator(String, Int)](data partition id to (node id to node index))
    // Transfer the original edges(node id) to new edges(node index)
    val ints: RDD[(Int, Int, Float)] = data.mapPartitionsWithIndex((partId, iterator) =>
      attachPartitionId(partId, iterator),
      true).join(partIndex).map { case (_, (sentences, mapping)) =>
      val map = new JHashMap[String, Long](mapping.size)
      for ((string, index) <- mapping) {
        map.put(string, index)
      }

      sentences.map(s => (map.get(s._1).toInt, map.get(s._2).toInt, s._3))
    }.flatMap(f => f)


    // Write the node id to node index map to hdfs
    saveRemappingResult(stringsWithIndex)

    ints
  }
}
