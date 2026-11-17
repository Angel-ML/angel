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
/**
 * Author: zhitaowang (zhitaowang@tecent.com)
 * Created At: 2021/03/01
 */
package com.tencent.angel.spark.ml.graph.algo.embedding.bge

import java.util.{Random, HashMap => JHashMap, HashSet => JHashSet}

import com.tencent.angel.graph.common.param.ModelContext
import com.tencent.angel.spark.context.{AngelPSContext, PSContext}
import com.tencent.angel.spark.ml.graph.algo.embedding.NEModel.NEDataSet
import com.tencent.angel.spark.ml.graph.algo.embedding.line.LINEModel.{LINEDataSet, buildDataBatches}
import com.tencent.angel.spark.ml.graph.algo.embedding.FastSigmoid
import com.tencent.angel.spark.ml.graph.utils.GraphIO
import com.tencent.angel.spark.ml.util.LogUtils
import it.unimi.dsi.fastutil.ints.{Int2IntOpenHashMap, Int2ObjectOpenHashMap}
import org.apache.hadoop.fs.Path
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Dataset
import org.apache.spark.storage.StorageLevel

import scala.collection.mutable.ArrayBuffer


/**
  * BGE mode used to handle no-weight graph
  *
  * @param dataset            training data set
  * @param embeddingDim       node embedding dimension
  * @param negativeNum        negative sample number
  * @param stepSize           learning rate
  * @param psPartNum          partition number of the model on PS
  * @param batchSize          size of mini-batch
  * @param epochNum           epoch number
  * @param dataPartNum        data partition number
  * @param srcNodeIdCol       source node column name
  * @param tgtNodeIdCol       target node column name
  * @param needMapping        remapping the node id to int or not
  * @param needSumSampling    need sub-sample
  * @param output             model and remapping result output path
  * @param checkpointInterval checkpoint interval
  * @param modelSaveInterval  mode save interval
  * @param extraInputEmbeddingPath extra src embedding for incremental training
  * @param extraOutputEmbeddingPath  extra tgt embedding for incremental training
 */
class BGEModel(dataset: Dataset[_], embeddingDim: Int, negativeNum: Int, stepSize: Float,
                psPartNum: Int, batchSize: Int, epochNum: Int, dataPartNum: Int, srcNodeIdCol: String,
               tgtNodeIdCol: String, needMapping: Boolean, needSumSampling: Boolean,
                output: String, checkpointInterval: Int, modelSaveInterval: Int,
               extraInputEmbeddingPath: String, extraOutputEmbeddingPath: String,
               useBalancePartition: Boolean) extends Serializable {

  /**
    * Model on PS
    */
  @volatile var psModel: BGEPSModel = null

  def train(): Unit = {
    // Original edges
    val newEdges = if (needMapping) {
      val edges = GraphIO.loadEdgesWithStringNodeId(dataset, srcNodeIdCol, tgtNodeIdCol)
      remapping(edges)
    } else {
      GraphIO.loadEdgesWithIntNodeId(dataset, srcNodeIdCol, tgtNodeIdCol)
    }

    newEdges.persist(StorageLevel.DISK_ONLY)


    // Get node id range and the number of total edges
    val (minId, maxId, numEdges) = newEdges.mapPartitions(summarizeApplyOp)
      .reduce(summarizeReduceOp)

    LogUtils.logTime(s"minId=${minId}, maxId=${maxId}, numEdges=${numEdges}, embedding dim=${embeddingDim}")

    // Start PS and init the model
    LogUtils.logTime("start to run ps")
    PSContext.getOrCreate(SparkContext.getOrCreate())

    // Create matrix and init PS model
    LogUtils.logTime("Create matrices in PS")
    var extraInputEmbeddingRDD: RDD[String] = null
    if (extraInputEmbeddingPath.length > 0) {
      extraInputEmbeddingRDD = newEdges.sparkContext.textFile(extraInputEmbeddingPath)
    }
    var extraOutputEmbeddingRDD: RDD[String] = null
    if (extraOutputEmbeddingPath.length > 0) {
      extraOutputEmbeddingRDD = newEdges.sparkContext.textFile(extraOutputEmbeddingPath)
    }

    initPSModel(minId, maxId, extraInputEmbeddingRDD, extraOutputEmbeddingRDD)

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
  def saveRemappingResult(stringsWithIndex: RDD[(String, Long)]) = {
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
  def checkpointAndSaveIfNeed(epoch: Int) = {
    var startTs = 0L
    if (epoch % checkpointInterval == 0 && epoch < epochNum) {
      LogUtils.logTime(s"Epoch=${epoch}, checkpoint the model")
      startTs = System.currentTimeMillis()
      getPSModel.checkpointEmbeddingMatrix(epoch)
      LogUtils.logTime(s"checkpoint use time=${System.currentTimeMillis() - startTs}")
    }

    if (epoch % modelSaveInterval == 0 && epoch < epochNum) {
      LogUtils.logTime(s"Epoch=${epoch}, save the model")
      startTs = System.currentTimeMillis()
      getPSModel.save(output, epoch)
      LogUtils.logTime(s"save use time=${System.currentTimeMillis() - startTs}")
    }
  }

  /**
    * Init the model on PS
    *
    * @param minId min node id
    * @param maxId max node id
    */
  def initPSModel(minId: Int, maxId: Int, extraInputEmbeddingRDD: RDD[String], extraOutputEmbeddingRDD: RDD[String]) = {
    val modelContext = new ModelContext(
      psPartNum, minId, maxId + 1, maxId - minId, "embedding", SparkContext.getOrCreate().hadoopConfiguration)
      psModel = BGEPSModel(modelContext, embeddingDim, isWeighted = false, extraInputEmbeddingRDD,
        extraOutputEmbeddingRDD, batchSize)
  }

  def getPSModel = psModel

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
    val tgtNodes = batch.dst
    val negativeSamples = getPSModel.negativeSample(srcNodes, tgtNodes, negativeNum, seed)
    val srcNegSamples = negativeSamples._1
    val tgtNegSamples = negativeSamples._2
    val sampleTime = System.currentTimeMillis() - start

    // Get node embedding from PS
    start = System.currentTimeMillis()
    val getResult = getPSModel.getEmbedding(srcNodes, tgtNodes, srcNegSamples, tgtNegSamples, negativeNum) // 修改负采样
    val srcFeats: Int2ObjectOpenHashMap[Array[Float]] = getResult._1
    val tgtFeats: Int2ObjectOpenHashMap[Array[Float]] = getResult._2

    val getEmbeddingTime = System.currentTimeMillis() - start

    // Get dot values
    start = System.currentTimeMillis()
    val dots = dot(srcNodes, tgtNodes, srcNegSamples, tgtNegSamples, srcFeats, tgtFeats)
    val dotTime = System.currentTimeMillis() - start

    // gradient
    start = System.currentTimeMillis()
    val loss = doGrad(dots, alpha)
    val gradientTime = System.currentTimeMillis() - start

    // Get the embedding updates
    start = System.currentTimeMillis()
    val (srcUpdates, tgtUpdates) = adjust(srcNodes, tgtNodes, srcNegSamples, tgtNegSamples, srcFeats, tgtFeats, dots) // 修改负采样
    val calUpdateTime = System.currentTimeMillis() - start

    // Push the updates to PS
    start = System.currentTimeMillis()
    getPSModel.adjust(srcUpdates, tgtUpdates)
    val pushTime = System.currentTimeMillis() - start

    if (batchId % 10 == 0) {
      LogUtils.logTime(s"loss=$loss sampleTime=${sampleTime} getEmbeddingTime=${getEmbeddingTime} " +
        s"dotTime=${dotTime} gradientTime=${gradientTime} calUpdateTime=${calUpdateTime} " +
        s"pushTime=${pushTime}")
    }

    (loss, dots.length.toLong, Array(sampleTime, getEmbeddingTime, dotTime, gradientTime, calUpdateTime, pushTime))
  }


  /**
    * Get dots values of node embedding vectors
    *
    * @param srcNodes        src nodes
    * @param tgtNodes       dst nodes
    * @param srcNegSamples  source negative samples
    * @param tgtNegSamples  target negative samples
    * @param srcFeats        src node to embedding vector map
    * @param tgtFeats     dst and negative samples to embedding vector map
    * @return
    */
  def dot(srcNodes: Array[Int], tgtNodes: Array[Int], srcNegSamples: Array[Array[Int]], tgtNegSamples: Array[Array[Int]],
          srcFeats: Int2ObjectOpenHashMap[Array[Float]], tgtFeats: Int2ObjectOpenHashMap[Array[Float]]): Array[Float] = {
    val dots: Array[Float] = new Array[Float]((1 + negativeNum * 2) * srcNodes.length)
    var docIndex = 0
    for (i <- 0 until srcNodes.length) {
      val srcVec = srcFeats.get(srcNodes(i))
      val tgtVec = tgtFeats.get(tgtNodes(i))
      // Get dot value for (src, dst)
      dots(docIndex) = arraysDot(srcVec, tgtVec)
      docIndex += 1
      // Get dot value for (src, target negative sample)
      for (j <- 0 until negativeNum) {
        dots(docIndex) = arraysDot(srcVec, tgtFeats.get(tgtNegSamples(i)(j)))
        docIndex += 1
      }
      // Get dot value for (target negative sample, tgt)
      for (j <- 0 until negativeNum) {
        dots(docIndex) = arraysDot(srcFeats.get(srcNegSamples(i)(j)), tgtVec)
        docIndex += 1
      }
    }
    dots

  }

  def arraysDot(x: Array[Float], y: Array[Float]): Float = {
    var dotValue = 0.0f
    (0 until x.length).foreach(i => dotValue += x(i) * y(i))
    dotValue
  }

  def axpy(y: Array[Float], x: Array[Float], a: Float) = {
    (0 until x.length).foreach(i => y(i) += a * x(i))
  }

  def div(x: Array[Float], f: Float): Unit = {
    (0 until x.length).foreach(i => x(i) = x(i) / f)
  }

  /**
    * Get the embedding vector updates for all nodes(src nodes, dst nodes and negative samples)
    *
    * @param srcNodes        source nodes
    * @param tgtNodes       target nodes
    * @param srcNegSamples source negative samples
    * @param tgtNegSamples target negative samples
    * @param srcFeats        src node to embedding vector map
    * @param tgtFeats     dst and negative samples to embedding vector map
    * @param dots            dot values
    * @return
    */
  def adjust(srcNodes: Array[Int], tgtNodes: Array[Int], srcNegSamples: Array[Array[Int]], tgtNegSamples: Array[Array[Int]],
             srcFeats: Int2ObjectOpenHashMap[Array[Float]], tgtFeats: Int2ObjectOpenHashMap[Array[Float]],
             dots: Array[Float]) = {

    val srcUpdateCounter = new Int2IntOpenHashMap(srcFeats.size())
    val srcUpdates = new Int2ObjectOpenHashMap[Array[Float]](srcFeats.size())

    val tgtUpdateCounter = new Int2IntOpenHashMap(tgtFeats.size())
    val tgtUpdates = new Int2ObjectOpenHashMap[Array[Float]](tgtFeats.size())

    var docIndex = 0
    for (i <- 0 until srcNodes.length) {
      // source node grad
      val srcGrad = new Array[Float](embeddingDim)
      // target node grad
      val tgtGrad = new Array[Float](embeddingDim)

      var g = dots(docIndex)
      val srcEmbedding = srcFeats.get(srcNodes(i))
      val tgtEmbedding = tgtFeats.get(tgtNodes(i))

      // Accumulate source node embedding to tgtGrad
      axpy(tgtGrad, srcEmbedding, g)
      // Accumulate target node embedding to srcGrad
      axpy(srcGrad, tgtEmbedding, g)

      docIndex += 1

      // Use src node embedding to update target negative sample node embedding; Accumulate target negative sample node embedding to srcGrad
      for (j <- 0 until negativeNum) {
        val tgtNegEmbedding = tgtFeats.get(tgtNegSamples(i)(j))
        g = dots(docIndex)
        // Accumulate target negative sample node embedding to srcGrad
        axpy(srcGrad, tgtNegEmbedding, g)
        // Use src node embedding to update target negative sample node embedding
        merge(tgtUpdateCounter, tgtUpdates, tgtNegSamples(i)(j), g, srcEmbedding)
        docIndex += 1
      }

      // Use tgt node embedding to update source negative sample node embedding; Accumulate source negative sample node embedding to tgtGrad
      for (j <- 0 until negativeNum) {
        val srcNegEmbedding = srcFeats.get(srcNegSamples(i)(j))
        g = dots(docIndex)
        // Accumulate source negative sample node embedding to tgtGrad
        axpy(tgtGrad, srcNegEmbedding, g)
        // Use tgt node embedding to update source negative sample node embedding
        merge(srcUpdateCounter, srcUpdates, srcNegSamples(i)(j), g, tgtEmbedding)
        docIndex += 1
      }

      // Use accumulation to update src node embedding, grad = 1
      merge(srcUpdateCounter, srcUpdates, srcNodes(i), 1, srcGrad)
      // Use accumulation to update tgt node embedding, grad = 1
      merge(tgtUpdateCounter, tgtUpdates, tgtNodes(i), 1, tgtGrad)
    }

    var iter = srcUpdateCounter.int2IntEntrySet().fastIterator()
    while (iter.hasNext) {
      val entry = iter.next()
      div(srcUpdates.get(entry.getIntKey), entry.getIntValue.toFloat)
    }

    iter = tgtUpdateCounter.int2IntEntrySet().fastIterator()
    while (iter.hasNext) {
      val entry = iter.next()
      div(tgtUpdates.get(entry.getIntKey), entry.getIntValue.toFloat)
    }

      (srcUpdates, tgtUpdates)

  }

  def merge(updateCounter: Int2IntOpenHashMap, updates: Int2ObjectOpenHashMap[Array[Float]],
            nodeId: Int, g: Float, value: Array[Float]) = {
    var grads: Array[Float] = updates.get(nodeId)
    if (grads == null) {
      grads = new Array[Float](embeddingDim)
      updates.put(nodeId, grads)
      updateCounter.put(nodeId, 0)
    }

    //grads.iaxpy(update, g)
    axpy(grads, value, g)
    updateCounter.addTo(nodeId, 1)
  }

  def doGrad(dots: Array[Float], alpha: Float): Float = {
    var loss = 0.0f
    for (i <- dots.indices) {
      val prob = FastSigmoid.sigmoid(dots(i))
      if (i % (2 * negativeNum + 1) == 0) {
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

  def save(outPath:String, epochNum:Int) = {
    psModel.save(outPath, epochNum)
  }
}

/**
  * BGE used to handle weighted graph
  *
  * @param dataset            training data set
  * @param embeddingDim       node embedding dimension
  * @param negativeNum        negative sample number
  * @param stepSize           learning rate
  * @param psPartNum          partition number of the model on PS
  * @param batchSize          size of mini-batch
  * @param epochNum           epoch number
  * @param dataPartNum        data partition number
  * @param srcNodeIdCol       src node column name
  * @param tgtNodeIdCol       dst node column name
  * @param weightCol          weight column name
  * @param needMapping        remapping the node id to int or not
  * @param needSumSampling    need sub-sample
  * @param output             model and remapping result output path
  * @param checkpointInterval checkpoint interval
  * @param modelSaveInterval  mode save interval
  */
class BGEWithWightModel(dataset: Dataset[_], embeddingDim: Int, negativeNum: Int, stepSize: Float,
                         psPartNum: Int, batchSize: Int, epochNum: Int, dataPartNum: Int, srcNodeIdCol: String,
                        tgtNodeIdCol: String, weightCol: String, needMapping: Boolean, needSumSampling: Boolean,
                         output: String, checkpointInterval: Int, modelSaveInterval: Int,
                        extraInputEmbeddingPath: String, extraOutputEmbeddingPath: String,
                        useBalancePartition: Boolean)
  extends BGEModel(dataset, embeddingDim, negativeNum, stepSize,
    psPartNum, batchSize, epochNum, dataPartNum, srcNodeIdCol,
    tgtNodeIdCol, needMapping, needSumSampling,
    output, checkpointInterval, modelSaveInterval,
    extraInputEmbeddingPath, extraOutputEmbeddingPath,
    useBalancePartition) {

  override def train() = {
    // Generate edge table with weight
    val edges = GraphIO.loadEdgesWithWeightStringNodeId(dataset, srcNodeIdCol, tgtNodeIdCol,
      weightCol, true, false)
    val newEdges: RDD[(Int, Int, Float)] = if (needMapping) {
      remappingWithWeight(edges)
    } else {
      edges.map(edge => (edge._1.toInt, edge._2.toInt, edge._3))
    }
    newEdges.persist(StorageLevel.DISK_ONLY)

    // Get the node index range and the edges number
    val (minId, maxId, numEdges) = newEdges.map(f => (f._1, f._2)).mapPartitions(summarizeApplyOp)
      .reduce(summarizeReduceOp)

    LogUtils.logTime(s"minId=${minId}, maxId=${maxId}, numEdges=${numEdges}, embedding dim=${embeddingDim}")

    // Start PS and init the model
    LogUtils.logTime("start to run ps")
    PSContext.getOrCreate(SparkContext.getOrCreate())

    // Create matrix and init PS model
    LogUtils.logTime("Create matrices in PS")
    var extraInputEmbeddingRDD: RDD[String] = null
    if (extraInputEmbeddingPath.length > 0) {
      extraInputEmbeddingRDD = newEdges.sparkContext.textFile(extraInputEmbeddingPath)
    }
    var extraOutputEmbeddingRDD: RDD[String] = null
    if (extraOutputEmbeddingPath.length > 0) {
      extraOutputEmbeddingRDD = newEdges.sparkContext.textFile(extraOutputEmbeddingPath)
    }

    initPSModel(minId, maxId, extraInputEmbeddingRDD, extraOutputEmbeddingRDD)

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
          sgdForBatch(batchSize, e._2, stepSize, seed + partId)
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
                  alpha: Float, seed: Int): (Float, Long, Array[Long]) = {

    var start = 0L

    // Sample the edge by weight
    start = System.currentTimeMillis()
    val batch = getPSModel.sampleEdges(batchSize, 1)
    val sampleWeightsTime = System.currentTimeMillis() - start

    start = System.currentTimeMillis()
    val srcNodes = batch._1
    val tgtNodes = batch._2

    val negativeSamples = getPSModel.negativeSample(srcNodes, tgtNodes, negativeNum, seed)
    val srcNegSamples = negativeSamples._1
    val tgtNegSamples = negativeSamples._2
    val sampleTime = System.currentTimeMillis() - start

    // Get node embedding from PS
    start = System.currentTimeMillis()
    val getResult = getPSModel.getEmbedding(srcNodes, tgtNodes, srcNegSamples, tgtNegSamples, negativeNum)   // 修改负采样
    val srcFeats: Int2ObjectOpenHashMap[Array[Float]] = getResult._1
    val tgtFeats: Int2ObjectOpenHashMap[Array[Float]] = getResult._2
    val getEmbeddingTime = System.currentTimeMillis() - start

    // Get dot values
    start = System.currentTimeMillis()
    val dots = dot(srcNodes, tgtNodes, srcNegSamples, tgtNegSamples, srcFeats, tgtFeats)
    val dotTime = System.currentTimeMillis() - start

    // gradient
    start = System.currentTimeMillis()
    val loss = doGrad(dots, alpha)
    val gradientTime = System.currentTimeMillis() - start

    start = System.currentTimeMillis()
    val (srcUpdates, tgtUpdates) = adjust(srcNodes, tgtNodes, srcNegSamples, tgtNegSamples, srcFeats, tgtFeats, dots)   // 修改负采样
    val calUpdateTime = System.currentTimeMillis() - start

    start = System.currentTimeMillis()
    getPSModel.adjust(srcUpdates, tgtUpdates)
    val pushTime = System.currentTimeMillis() - start

    if (batchId % 10 == 0) {
      LogUtils.logTime(s"loss=$loss sampleWeightsTime=${sampleWeightsTime}, sampleTime=${sampleTime} getEmbeddingTime=${getEmbeddingTime} " +
        s"dotTime=${dotTime} gradientTime=${gradientTime} calUpdateTime=${calUpdateTime} " +
        s"pushTime=${pushTime}")
    }

    (loss, dots.length.toLong, Array(sampleWeightsTime, sampleTime, getEmbeddingTime, dotTime, gradientTime, calUpdateTime, pushTime))
  }

  override def initPSModel(minId: Int, maxId: Int, extraInputEmbeddingRDD: RDD[String], extraOutputEmbeddingRDD: RDD[String]) = {
    val modelContext = new ModelContext(
      psPartNum, minId, maxId + 1, maxId - minId, "embedding", SparkContext.getOrCreate().hadoopConfiguration)
    psModel = BGEPSModel(modelContext, embeddingDim, isWeighted = true, extraInputEmbeddingRDD, extraOutputEmbeddingRDD, batchSize)
  }

  override def getPSModel: BGEWithWeightPSModel = super.getPSModel.asInstanceOf[BGEWithWeightPSModel]


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
