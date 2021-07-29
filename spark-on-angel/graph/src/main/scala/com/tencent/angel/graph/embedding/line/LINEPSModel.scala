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

package com.tencent.angel.graph.embedding.line

import java.io.IOException
import java.text.SimpleDateFormat
import java.util.Date
import java.util.concurrent.TimeUnit

import com.tencent.angel.graph.common.param.ModelContext
import com.tencent.angel.graph.common.psf.param.IntKeysUpdateParam
import com.tencent.angel.graph.utils.ModelContextUtils
import com.tencent.angel.ml.matrix.psf.update.base.VoidResult
import com.tencent.angel.ml.matrix.{MatrixContext, RowType}
import com.tencent.angel.model.output.format.{MatrixFilesMeta, ModelFilesConstent}
import com.tencent.angel.model.{MatrixLoadContext, MatrixSaveContext, ModelLoadContext, ModelSaveContext}
import com.tencent.angel.ps.storage.vector.element.IElement
import com.tencent.angel.spark.context.{AngelPSContext, PSContext}
import com.tencent.angel.spark.ml.util.LogUtils
import com.tencent.angel.spark.models.PSMatrix
import it.unimi.dsi.fastutil.ints.Int2ObjectOpenHashMap
import org.apache.hadoop.fs.Path
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

import scala.util.Random

/**
  * Base class of LINE PS Model
  *
  * @param embeddingMatrix embedding matrix
  */
class LINEPSModel(embeddingMatrix: PSMatrix, modelContext: ModelContext) extends AlgoPSModel {
  checkpointContext.addReadWriteMatrix(embeddingMatrix)

  /**
    * Negative sample for a batch nodes
    *
    * @param nodeIds   src node ids
    * @param sampleNum sample number per node
    * @param seed      random seed
    * @return sample results
    */
  def negativeSample(nodeIds: Array[Int], dstNodeIds: Array[Int], sampleNum: Int, seed: Int): Array[Array[Int]] = {
    //val seed = UUID.randomUUID().hashCode()
    val rand = new Random(seed)
    val sampleNodes = new Array[Array[Int]](nodeIds.length)
    var nodeIndex: Int = 0

    val maxNodeId = modelContext.getMaxNodeId.toInt
    val minNodeId = modelContext.getMinNodeId.toInt
    for (i <- nodeIds.indices) {
      var sampleIndex: Int = 0
      sampleNodes(nodeIndex) = new Array[Int](sampleNum)
      while (sampleIndex < sampleNum) {
        val target = rand.nextInt(maxNodeId - minNodeId) + minNodeId
        if (target != nodeIds(i) && target != dstNodeIds(i)) {
          sampleNodes(nodeIndex)(sampleIndex) = target
          sampleIndex += 1
        }
      }
      nodeIndex += 1
    }
    sampleNodes
  }

  /**
    * Push the update to PS
    *
    * @param inputUpdates  src node embedding updates
    * @param outputUpdates dst node embedding updates
    * @param order         order
    * @return future object for async
    */
  def adjust(inputUpdates: Int2ObjectOpenHashMap[Array[Float]], outputUpdates: Int2ObjectOpenHashMap[Array[Float]], order: Int): VoidResult = {
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
  def getEmbedding(srcNodes: Array[Int], destNodes: Array[Int], negativeSamples: Array[Array[Int]], negative: Int, order: Int): (Int2ObjectOpenHashMap[Array[Float]], Int2ObjectOpenHashMap[Array[Float]]) = {
    embeddingMatrix.asyncPsfGet(new LINEGetEmbedding(new LINEGetEmbeddingParam(embeddingMatrix.id, srcNodes, destNodes,
      negativeSamples, order, negative))).get(600000, TimeUnit.MILLISECONDS).asInstanceOf[LINEGetEmbeddingResult].getResult
  }

  /**
    * Initialize the embedding vector on PS
    *
    * @param seed      random seed
    * @param dimension embedding vector dimension
    * @param order     order
    */
  def randomInitialize(seed: Int, dimension: Int, order: Int): Unit = {
    if (modelContext.isUseHashPartition) {
      // Init as mini-batch
      modelContext.getMinNodeId.toInt.to(modelContext.getMaxNodeId.toInt).sliding(1000000, 1000000).foreach(e => {
        embeddingMatrix.asyncPsfUpdate(new LINEModelRandomizeAsNodes(
          new RandomizeUpdateAsNodesParam(embeddingMatrix.id, dimension, e.toArray, order, seed)))
          .get(60000, TimeUnit.MILLISECONDS)
      })
      val beforeRandomize = System.currentTimeMillis()

      logTime(s"Model successfully Randomized, cost ${(System.currentTimeMillis() - beforeRandomize) / 1000.0}s")
    } else {
      // Just init by range
      val beforeRandomize = System.currentTimeMillis()
      embeddingMatrix.asyncPsfUpdate(new LINEModelRandomize(
        new RandomizeUpdateParam(embeddingMatrix.id, dimension, order, seed)))
        .get(1800000, TimeUnit.MILLISECONDS)
      logTime(s"Model successfully Randomized, cost ${(System.currentTimeMillis() - beforeRandomize) / 1000.0}s")
    }
  }

  def extraInitialize(extraInputRDD: RDD[String], extraContextRDD: RDD[String], batchSize: Int, order: Int): Unit = {
    val conf = SparkContext.getOrCreate().getConf
    val keyValueSep = conf.get("spark.hadoop.angel.line.keyvalue.sep", "colon") match {
      case "space" => " "
      case "comma" => ","
      case "tab" => "\t"
      case "colon" => ":"
      case "bar" => "\\|"
    }
    val featSep = conf.get("spark.hadoop.angel.line.feature.sep", "space") match {
      case "space" => " "
      case "comma" => ","
      case "tab" => "\t"
      case "colon" => ":"
      case "bar" => "\\|"
    }
    var beforeInitialize = System.currentTimeMillis()
    if (extraInputRDD != null) {
      extraInputRDD.mapPartitions { iterator =>
        iterator.sliding(batchSize, batchSize)
          .foreach(batch => extraUpdate(batch.toArray, order, keyValueSep, featSep, true))
        Iterator.single()
      }.count()
    }
    LogUtils.logTime(s"Model successfully extra Initial input embedding, " +
      s"cost ${(System.currentTimeMillis() - beforeInitialize) / 1000.0}s")
    if (extraContextRDD != null && order == 2) {
      beforeInitialize = System.currentTimeMillis()
      extraContextRDD.mapPartitions { iterator =>
        iterator.sliding(batchSize, batchSize)
          .foreach(batch => extraUpdate(batch.toArray, order, keyValueSep, featSep, false))
        Iterator.single()
      }.count()
      LogUtils.logTime(s"Model successfully extra Initial output embedding, " +
        s"cost ${(System.currentTimeMillis() - beforeInitialize) / 1000.0}s")
    }
  }

  def extraUpdate(strings: Array[String], order: Int, keyValueSep: String, featSep: String, isInput: Boolean): Unit = {
    val updates = new Int2ObjectOpenHashMap[Array[Float]]()
    if (keyValueSep.equals(featSep)) {
      strings.map { line =>
        val splits = line.split(keyValueSep)
        val key = splits(0).toInt
        val value = splits.slice(1, splits.length).map(v => v.toFloat)
        updates.put(key, value)
      }
    } else {
      strings.map { line =>
        val splits = line.split(keyValueSep)
        val key = splits(0).toInt
        val value = splits(1).split(featSep).map(v => v.toFloat)
        updates.put(key, value)
      }
    }
    if (isInput) {
      embeddingMatrix.psfUpdate(new LINEAdjust(new LINEAdjustParam(embeddingMatrix.id, updates, null,
        order, true)))
    } else {
      embeddingMatrix.psfUpdate(new LINEAdjust(new LINEAdjustParam(embeddingMatrix.id, null, updates,
        order, true)))
    }

  }

  /**
    * Dump the embedding matrix on PS to HDFS
    *
    * @param checkpointId checkpoint id
    */
  def checkpointEmbeddingMatrix(checkpointId: Int): Unit = {
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
  def save(modelPathRoot: String, epoch: Int, saveContextEmbedding: Boolean, order: Int): Unit = {
    save(new Path(modelPathRoot, s"CP_$epoch").toString, saveContextEmbedding, order)
  }

  /**
    * Save the model on PS to hdfs
    *
    * @param modelPath save path
    */
  def save(modelPath: String, saveContextEmbedding: Boolean, order: Int): Unit = {
    logTime(s"saving model to $modelPath")
    val ss = SparkSession.builder().getOrCreate()

    // Delete if exist
    deleteIfExists(modelPath, ss)

    // Save Model
    if (saveContextEmbedding && order == 2) {
      // Save input and context embedding use "TextLINEModelOutputFormat" and "TextLINEModelContextEmbOutputFormat"
      val inputEmbSaveContext = new ModelSaveContext(new Path(modelPath, s"inputEmbedding").toString)
      inputEmbSaveContext.addMatrix(new MatrixSaveContext(embeddingMatrix.name, classOf[TextLINEModelOutputFormat].getTypeName))
      PSContext.instance().save(inputEmbSaveContext)
      val outputEmbSaveContext = new ModelSaveContext(new Path(modelPath, s"contextEmbedding").toString)
      outputEmbSaveContext.addMatrix(new MatrixSaveContext(embeddingMatrix.name, classOf[TextLINEModelContextEmbOutputFormat].getTypeName))
      PSContext.instance().save(outputEmbSaveContext)
      deleteMetaIfExists(new Path(modelPath, s"inputEmbedding").toString)
      deleteMetaIfExists(new Path(modelPath, s"contextEmbedding").toString)
    } else {
      // Save use "TextLINEModelOutputFormat" format
      val saveContext = new ModelSaveContext(modelPath)
      saveContext.addMatrix(new MatrixSaveContext(embeddingMatrix.name, classOf[TextLINEModelOutputFormat].getTypeName))
      PSContext.instance().save(saveContext)
      deleteMetaIfExists(modelPath)
    }
  }

  private def deleteMetaIfExists(modelPath: String): Unit = {
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
  val neighborTable = "neighborTable"
  val aliasTable = "aliasTable"

  def apply(modelContext: ModelContext, order: Int, dimension: Int,
            isWeighted: Boolean, extraInputEmbeddingRDD: RDD[String], extraContextEmbeddingRDD: RDD[String],
            batchSize: Int): LINEPSModel = {
    LogUtils.logTime(s"Init model, min id = ${modelContext.getMinNodeId} " +
      s"and max id = ${modelContext.getMaxNodeId}")
    // Create a matrix for embedding vector
    val embeddingContext: MatrixContext = ModelContextUtils.createMatrixContext(modelContext,
      embedding, RowType.T_ANY_INTKEY_SPARSE, classOf[LINENode])

    // If use range partition, as id is in int range and continues, we use dense format to speed up data access
    if (modelContext.isUseRangePartition) {
      embeddingContext.setRowType(RowType.T_ANY_INTKEY_DENSE)
    }
    val embeddingMatrix: PSMatrix = PSMatrix.matrix(embeddingContext)

    var model: LINEPSModel = null
    if (isWeighted) {
      // Create a matrix for alias table
      val aliasTableContext = ModelContextUtils.createMatrixContextWithUserDefinePartition(
        modelContext, aliasTable, RowType.T_INT_SPARSE, classOf[EdgeAliasTablePartition])

      val aliasTableMatrix: PSMatrix = PSMatrix.matrix(aliasTableContext)

      model = new LINEWithWeightPSModel(embeddingMatrix, embeddingMatrix, aliasTableMatrix, modelContext)
    } else {
      model = new LINEPSModel(embeddingMatrix, modelContext)
    }
    // init model
    if (extraInputEmbeddingRDD != null || extraContextEmbeddingRDD != null) {
      model.randomInitialize(model.hashCode(), dimension, order)
      model.extraInitialize(extraInputEmbeddingRDD, extraContextEmbeddingRDD, batchSize, order)
    } else {
      model.randomInitialize(model.hashCode(), dimension, order)
    }
    model
  }
}

/**
  * PS model for weighted line
  *
  * @param embeddingMatrix     embedding matrix
  * @param neighborTableMatrix neighbor table matrix
  * @param aliasTableMatrix    alias table matrix
  */
class LINEWithWeightPSModel(embeddingMatrix: PSMatrix, neighborTableMatrix: PSMatrix, aliasTableMatrix: PSMatrix,
                            modelContext: ModelContext) extends LINEPSModel(embeddingMatrix: PSMatrix, modelContext) {

  /**
    * Notice the PS that all neighbors are push, PS will build the alias table
    *
    * @return
    */
  def initNeighborsOver(): VoidResult = {
    neighborTableMatrix.asyncPsfUpdate(new InitAliasTable(new InitAliasTableParam(neighborTableMatrix.id,
      aliasTableMatrix.id))).get(1800000, TimeUnit.MILLISECONDS)
  }

  /**
    * Push the neighbor table to PS for a mini-batch nodes
    *
    * @param pairs nodes to neighbor table map
    * @return
    */
  def initNeighbors(pairs: Seq[(Int, Iterable[(Int, Float)])]): VoidResult = {
    var index = 0
    val nodeIds = new Array[Int](pairs.size)
    val edgeWightPairs = new Array[IElement](pairs.size)

    pairs.foreach(pair => {
      nodeIds(index) = pair._1
      val iter = pair._2
      val neighbors = new Array[Int](iter.size)
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
        new IntKeysUpdateParam(
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
  def sampleEdges(batchSize: Int, sampleBatchSize: Int): (Array[Int], Array[Int]) = {
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
  def checkpointEmbeddingAndAliasTable(checkpointId: Int): Unit = {
    val matrixNames = new Array[String](2)
    matrixNames(0) = embeddingMatrix.name
    matrixNames(1) = aliasTableMatrix.name
    CheckpointUtils.checkpoint(checkpointId, matrixNames)
  }
}
