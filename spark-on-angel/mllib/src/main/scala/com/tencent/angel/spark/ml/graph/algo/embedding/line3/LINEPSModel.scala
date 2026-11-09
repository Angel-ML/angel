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

package com.tencent.angel.spark.ml.graph.algo.embedding.line3

import java.io.IOException
import java.text.SimpleDateFormat
import java.util.Date
import java.util.concurrent.TimeUnit

import com.tencent.angel.common.collections.DummyElement
import com.tencent.angel.graph.common.param.ModelContext
import com.tencent.angel.graph.common.psf.param.{AnyKeysUpdateParam, IntKeysUpdateParam}
import com.tencent.angel.ml.matrix.psf.update.base.VoidResult
import com.tencent.angel.ml.matrix.{MatrixContext, RowType}
import com.tencent.angel.model.output.format.{MatrixFilesMeta, ModelFilesConstent}
import com.tencent.angel.model.{MatrixLoadContext, MatrixSaveContext, ModelLoadContext, ModelSaveContext}
import com.tencent.angel.ps.storage.partitioner.HashPartitioner
import com.tencent.angel.ps.storage.vector.element.IElement
import com.tencent.angel.psagent.matrix.ResponseType
import com.tencent.angel.spark.context.{AngelPSContext, PSContext}
import com.tencent.angel.spark.ml.util.LogUtils
import com.tencent.angel.spark.models.PSMatrix
import it.unimi.dsi.fastutil.objects.Object2ObjectOpenHashMap
import org.apache.hadoop.fs.Path
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

/**
  * Base class of LINE PS Model
  *
  * @param embeddingMatrix embedding matrix
  */
class LINEPSModel(embeddingMatrix: PSMatrix,
                  nodeTableMatrix: PSMatrix,
                  modelContext: ModelContext) extends AlgoPSModel {
  checkpointContext.addReadWriteMatrix(embeddingMatrix)

  /**
    * Negative sample for a batch nodes
    *
    * @param nodeIds   src node ids
    * @param sampleNum sample number per node
    * @param seed      random seed
    * @return sample results
    */
  def negativeSample(nodeIds: Array[ReadOnlyByteArray],
                     dstNodeIds: Array[ReadOnlyByteArray],
                     sampleNum: Int,
                     seed: Int): Array[Array[ReadOnlyByteArray]] = {
    nodeTableMatrix.asyncPsfGet(
      new NegativeSample(
        new NegativeSampleParam(nodeTableMatrix.id, nodeIds, dstNodeIds, sampleNum)))
      .get(60000, TimeUnit.MILLISECONDS)
      .asInstanceOf[NegativeSampleResult].batchSamples
  }

  /**
    * Push the update to PS
    *
    * @param inputUpdates  src node embedding updates
    * @param outputUpdates dst node embedding updates
    * @param order         order
    * @return future object for async
    */
  def adjust(inputUpdates: Object2ObjectOpenHashMap[ReadOnlyByteArray, Array[Float]],
             outputUpdates: Object2ObjectOpenHashMap[ReadOnlyByteArray, Array[Float]],
             order: Int) = {
    embeddingMatrix.asyncPsfUpdate(new LINEAdjust(
      new LINEAdjustParam(embeddingMatrix.id, inputUpdates, outputUpdates, order))).get(600000, TimeUnit.MILLISECONDS)
  }

  /**
    * Get node embedding vectors
    *
    * @param srcNodes        src nodes
    * @param destNodes       dst nodes
    * @param negativeSamples negative sample nodes
    * @param negative        negative sample number per node
    * @param order           order
    * @return node id to embedding vector map
    */
  def getEmbedding(srcNodes: Array[ReadOnlyByteArray],
                   destNodes: Array[ReadOnlyByteArray],
                   negativeSamples: Array[Array[ReadOnlyByteArray]],
                   negative: Int,
                   order: Int) = {
    embeddingMatrix.asyncPsfGet(
      new LINEGetEmbedding(
        new LINEGetEmbeddingParam(embeddingMatrix.id, srcNodes, destNodes,
          negativeSamples, order, negative))).get(600000, TimeUnit.MILLISECONDS)
      .asInstanceOf[LINEGetEmbeddingResult].getResult
  }

  /**
    * Initialize the embedding vector on PS
    *
    * @param seed      random seed
    * @param dimension embedding vector dimension
    * @param order     order
    */
  def randomInitialize(neighborTable: RDD[ReadOnlyByteArray],
                       seed: Int, dimension: Int, order: Int) = {

    neighborTable.mapPartitions(iter => {
      iter.sliding(1000000, 1000000).map(batch => {
        embeddingMatrix.asyncPsfUpdate(new LINEModelRandomizeAsNodes(
          new RandomizeUpdateAsNodesParam(
            embeddingMatrix.id, dimension, batch.toArray, order, seed)))
          .get(60000, TimeUnit.MILLISECONDS)
      })
    }).count()

    // Build nodes partition
    embeddingMatrix.psfUpdate(
      new InitNodeTable(
        new InitNodeTableParam(embeddingMatrix.id, nodeTableMatrix.id))).get(60000, TimeUnit.MILLISECONDS)
  }

  def extraInitialize(extraRDD: RDD[String], batchSize: Int, order: Int): Unit = {
    val beforeInitialize = System.currentTimeMillis()
    extraRDD.mapPartitions { iterator =>
      iterator.sliding(batchSize, batchSize)
        .map(batch => extraUpdate(batch.toArray, order))
    }.count()
    LogUtils.logTime(s"Model successfully extra Initial, cost ${(System.currentTimeMillis() - beforeInitialize) / 1000.0}s")
  }

  def extraUpdate(strings: Array[String], order: Int): Unit = {
    val inputUpdates = new Object2ObjectOpenHashMap[ReadOnlyByteArray, Array[Float]]()
    strings.map { line =>
      val splits = line.split(":")
      val key = new ReadOnlyByteArray(splits(0).getBytes())
      val value = splits(1).split(" ").map(v => v.toFloat)
      inputUpdates.put(key, value)
    }
    embeddingMatrix.psfUpdate(
      new LINEAdjust(
        new LINEAdjustParam(embeddingMatrix.id, inputUpdates, null, order, true))).get()
  }

  /**
    * Dump the embedding matrix on PS to HDFS
    *
    * @param checkpointId checkpoint id
    */
  def checkpointEmbeddingMatrix(checkpointId: Int) = {
    val matrixNames = new Array[String](1)
    matrixNames(0) = embeddingMatrix.name
    CheckpointUtils.checkpoint(checkpointId, matrixNames)
  }

  /**
    * Save the model on PS to hdfs
    *
    * @param modelPathRoot model save root path
    * @param epoch         epoch index
    */
  def save(modelPathRoot: String, epoch: Int, saveMeta: Boolean): Unit = {
    save(new Path(modelPathRoot, s"CP_$epoch").toString, saveMeta)
  }

  /**
    * Save the model on PS to hdfs
    *
    * @param modelPath save path
    */
  def save(modelPath: String, saveMeta: Boolean): Unit = {
    logTime(s"saving model to $modelPath")
    val ss = SparkSession.builder().getOrCreate()

    // Delete if exist
    deleteIfExists(modelPath, ss)

    // Save use "TextLINEModelOutputFormat" format
    val saveContext = new ModelSaveContext(modelPath)
    saveContext.addMatrix(new MatrixSaveContext(embeddingMatrix.name, classOf[TextLINEModelOutputFormat].getTypeName))
    PSContext.instance().save(saveContext)


    if (!saveMeta) {
      // Remove the meta file
      try {
        val metaPath = new Path(new Path(modelPath, embeddingMatrix.name), ModelFilesConstent.modelMetaFileName)
        // Build hadoop conf
        val conf = AngelPSContext.convertToHadoop(SparkContext.getOrCreate().getConf)
        val fs = metaPath.getFileSystem(conf)
        // Remove
        val ret = fs.delete(metaPath, true)
        if (!ret) {
          logTime(s"Warning: remove meta file failed !!!")
        }
      } catch {
        case e: Throwable => logTime(s"Warning: remove meta file failed !!!")
      }
    }
  }

  private def deleteIfExists(modelPath: String, ss: SparkSession): Unit = {
    val path = new Path(modelPath)
    val fs = path.getFileSystem(ss.sparkContext.hadoopConfiguration)
    if (fs.exists(path)) {
      fs.delete(path, true)
    }
  }

  /**
    * Load model from hdfs to PS
    *
    * @param modelPath model save path
    */
  def load(modelPath: String): Unit = {
    val startTime = System.currentTimeMillis()
    logTime(s"load model from $modelPath")

    val loadContext = new ModelLoadContext(modelPath)
    loadContext.addMatrix(new MatrixLoadContext(embeddingMatrix.name))
    PSContext.getOrCreate(SparkContext.getOrCreate()).load(loadContext)
    logTime(s"model load time=${System.currentTimeMillis() - startTime} ms")
  }

  def logTime(msg: String): Unit = {
    val time = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date)
    println(s"[$time] $msg")
  }
}

object LINEPSModel {
  val embedding = "embedding"
  val nodeTable = "nodeTable"
  val neighborTable = "neighborTable"
  val aliasTable = "aliasTable"

  def apply(modelContext: ModelContext, order: Int, batchSize: Int, hasWeight: Boolean = false): LINEPSModel = {
    val embeddingContext: MatrixContext = new MatrixContext(embedding, 1, -1)
    embeddingContext.setValidIndexNum(modelContext.getNodeNum)
    embeddingContext.setValueType(classOf[LINENode])
    //embeddingContext.setInitFunc(new LINEInitFunc(order, dimension))
    embeddingContext.setPartitionNum(modelContext.getPartitionNum)

    embeddingContext.setRowType(RowType.T_ANY_ANYKEY_SPARSE)
    embeddingContext.setPartitionerClass(classOf[HashPartitioner])

    if(!hasWeight) {
      val nodeTableContext: MatrixContext = new MatrixContext(nodeTable, 1, -1)
      nodeTableContext.setValidIndexNum(modelContext.getNodeNum)
      nodeTableContext.setPartitionNum(modelContext.getPartitionNum)
      nodeTableContext.setPartitionClass(classOf[NodeTablePartition])
      nodeTableContext.setRowType(RowType.T_ANY_ANYKEY_SPARSE)
      nodeTableContext.setValueType(classOf[DummyElement])
      nodeTableContext.setPartitionerClass(classOf[HashPartitioner])

      val embeddingMatrix = PSMatrix.matrix(embeddingContext)
      val nodeMatrix = PSMatrix.matrix(nodeTableContext)
      val model = new LINEPSModel(embeddingMatrix, nodeMatrix, modelContext)
      model
    } else {
      val aliasTableContext: MatrixContext = new MatrixContext(aliasTable, 1, -1)
      aliasTableContext.setValidIndexNum(modelContext.getNodeNum)
      aliasTableContext.setPartitionNum(modelContext.getPartitionNum)
      aliasTableContext.setPartitionClass(classOf[EdgeAliasTablePartition])
      aliasTableContext.setRowType(RowType.T_ANY_ANYKEY_SPARSE)
      aliasTableContext.setValueType(classOf[DummyElement])
      aliasTableContext.setPartitionerClass(classOf[HashPartitioner])

      val embeddingMatrix = PSMatrix.matrix(embeddingContext)
      val aliasTableMatrix = PSMatrix.matrix(aliasTableContext)
      val model = new LINEWithWeightPSModel(embeddingMatrix, embeddingMatrix, aliasTableMatrix, modelContext)
      model
    }
  }

  def getMaxId(oldModelPath: String): Long = {
    val meteFilePath = new Path(new Path(oldModelPath, LINEPSModel.embedding), ModelFilesConstent.modelMetaFileName)
    val meta = new MatrixFilesMeta
    val conf = AngelPSContext.convertToHadoop(SparkContext.getOrCreate().getConf)
    val fs = meteFilePath.getFileSystem(conf)
    if (!fs.exists(meteFilePath)) throw new IOException("matrix meta file does not exist ")
    val input = fs.open(meteFilePath)
    try
      meta.read(input)
    catch {
      case e: Throwable =>
        throw new IOException("Read meta failed ", e)
    } finally input.close()
    val colNum = meta.getCol
    LogUtils.logTime("Load model max col is: " + colNum)
    colNum
  }
}

/**
  * PS model for weighted line
  *
  * @param embeddingMatrix     embedding matrix
  * @param neighborTableMatrix neighbor table matrix
  * @param aliasTableMatrix    alias table matrix
  * @param minNodeId           min node id
  * @param maxNodeId           max node id
  */
class LINEWithWeightPSModel(embeddingMatrix: PSMatrix,
                            neighborTableMatrix: PSMatrix,
                            aliasTableMatrix: PSMatrix,
                            modelContext: ModelContext) extends LINEPSModel(embeddingMatrix, neighborTableMatrix, modelContext) {

  /**
    * Negative sample for a batch nodes
    *
    * @param nodeIds   src node ids
    * @param sampleNum sample number per node
    * @param seed      random seed
    * @return sample results
    */
  override def negativeSample(nodeIds: Array[ReadOnlyByteArray],
                     dstNodeIds: Array[ReadOnlyByteArray],
                     sampleNum: Int,
                     seed: Int): Array[Array[ReadOnlyByteArray]] = {
    aliasTableMatrix.asyncPsfGet(
      new NegativeSample(
        new NegativeSampleParam(aliasTableMatrix.id, nodeIds, dstNodeIds, sampleNum)))
      .get(60000, TimeUnit.MILLISECONDS)
      .asInstanceOf[NegativeSampleResult].batchSamples
  }

  /**
    * Initialize the embedding vector on PS
    *
    * @param seed      random seed
    * @param dimension embedding vector dimension
    * @param order     order
    */
  override def randomInitialize(neighborTable: RDD[ReadOnlyByteArray],
                       seed: Int, dimension: Int, order: Int) = {

    neighborTable.mapPartitions(iter => {
      iter.sliding(1000000, 1000000).map(batch => {
        embeddingMatrix.asyncPsfUpdate(new LINEModelRandomizeAsNodes(
          new RandomizeUpdateAsNodesParam(
            embeddingMatrix.id, dimension, batch.toArray, order, seed)))
          .get(60000, TimeUnit.MILLISECONDS)
      })
    }).count()

    val ret = new VoidResult(ResponseType.SUCCESS)
    ret
  }

  /**
    * Notice the PS that all neighbors are push, PS will build the alias table
    *
    * @return
    */
  def initNeighborsOver() = {
    neighborTableMatrix.asyncPsfUpdate(
      new InitAliasTable(
        new InitAliasTableParam(neighborTableMatrix.id, aliasTableMatrix.id))).get(1800000, TimeUnit.MILLISECONDS)
  }

  /**
    * Push the neighbor table to PS for a mini-batch nodes
    *
    * @param pairs nodes to neighbor table map
    * @return
    */
  def initNeighbors(pairs: Seq[(ReadOnlyByteArray, Iterable[(ReadOnlyByteArray, Float)])]) = {
    var index = 0
    val nodeIds = new Array[IElement](pairs.size)
    val edgeWightPairs = new Array[IElement](pairs.size)

    pairs.foreach(pair => {
      nodeIds(index) = pair._1
      val iter = pair._2
      val neighbors = new Array[ReadOnlyByteArray](iter.size)
      val weights = new Array[Float](iter.size)
      var neighborIndex = 0
      iter.foreach(it => {
        neighbors(neighborIndex) = it._1
        weights(neighborIndex) = it._2
        neighborIndex += 1
      })
      edgeWightPairs(index) = new EdgeWeightPairs(neighbors, weights)
      index += 1
    })

    neighborTableMatrix.asyncPsfUpdate(
      new InitEdgeWeight(
        new AnyKeysUpdateParam(
          neighborTableMatrix.id, nodeIds, edgeWightPairs)))
      .get(1800000, TimeUnit.MILLISECONDS)
  }

  /**
    * Sample a mini-batch edges
    *
    * @param batchSize       sample number
    * @param sampleBatchSize sample batch size
    * @return sampled edges
    */
  def sampleEdges(batchSize: Int, sampleBatchSize: Int) = {
    val table = PSPartitionAliasTable.get(aliasTableMatrix)
    val sampleResult = aliasTableMatrix.asyncPsfGet(new SampleWithWeight(new SampleWithWeightParam(aliasTableMatrix.id,
      batchSize, sampleBatchSize, table))).get(600000, TimeUnit.MILLISECONDS).asInstanceOf[SampleWithWeightResult]
    (sampleResult.getSrcNodes, sampleResult.getDstNodes)
  }

  /**
    * Dump the alias table on PS to HDFS
    *
    * @param checkpointId checkpoint id
    */
  def checkpointEmbeddingAndAliasTable(checkpointId: Int) = {
    val matrixNames = new Array[String](2)
    matrixNames(0) = embeddingMatrix.name
    matrixNames(1) = aliasTableMatrix.name
    CheckpointUtils.checkpoint(checkpointId, matrixNames)
  }
}
