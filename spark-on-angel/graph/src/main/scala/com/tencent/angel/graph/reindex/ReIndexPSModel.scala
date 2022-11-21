package com.tencent.angel.graph.reindex

import com.tencent.angel.graph.common.param.ModelContext
import com.tencent.angel.ml.matrix.{MatrixContext, RowType}
import com.tencent.angel.ps.storage.partitioner.HashPartitioner
import com.tencent.angel.graph.embedding.line.{AlgoPSModel, CheckpointUtils}
import com.tencent.angel.spark.ml.util.LogUtils
import com.tencent.angel.spark.models.PSMatrix
import it.unimi.dsi.fastutil.objects.Object2LongOpenHashMap
import org.apache.spark.rdd.RDD

import java.util.concurrent.TimeUnit

class ReIndexPSModel(mappingMatrix: PSMatrix) extends AlgoPSModel {
  checkpointContext.addReadWriteMatrix(mappingMatrix)

  def initialize(extraRDD: RDD[(ReadOnlyByteArray, Long)], batchSize: Int): Unit = {
    val beforeInitialize = System.currentTimeMillis()
    extraRDD.mapPartitions { iterator =>
      iterator.sliding(batchSize, batchSize)
        .map(batch => batchUpdate(batch.toArray))
    }.count()
    LogUtils.logTime(s"mapping Model successfully Initial, cost ${(System.currentTimeMillis() - beforeInitialize) / 1000.0}s")
    //val rowSize = mappingMatrix.psfGet(new InitCheck(new InitCheckParam(mappingMatrix.id))).asInstanceOf[GetInitCheckResult].getResult
    //println("after init ps row storage total size is: " + rowSize)
  }

  def batchUpdate(batchMaps: Array[(ReadOnlyByteArray, Long)]): Unit = {
    val updates = new Object2LongOpenHashMap[ReadOnlyByteArray]()
    batchMaps.map { ele =>
      updates.put(ele._1, ele._2)
    }
    mappingMatrix.asyncPsfUpdate(new InitAsNodes(new InitAsNodesParam(mappingMatrix.id, updates))).get(600000, TimeUnit.MILLISECONDS)
  }

  def getValues(keysRDD: RDD[(ReadOnlyByteArray, ReadOnlyByteArray)], batchSize: Int): RDD[(Long, Long)] = {
    val results =  keysRDD.mapPartitions { iterator =>
      iterator.sliding(batchSize, batchSize)
        .map(batch => batchGet(batch.toArray))
    }.flatMap(x => x)
    results
  }

  def getValuesWithSentences(keysRDD: RDD[Array[ReadOnlyByteArray]], batchSize: Int): RDD[Array[Int]] = {
    val results =  keysRDD.mapPartitions { iterator =>
      iterator.sliding(batchSize, batchSize)
        .map(batch => batchGetWithSentences(batch.toArray))
    }.flatMap(x => x)
    results
  }

  def batchGetWithSentences(batchNodes: Array[Array[ReadOnlyByteArray]]): Iterator[Array[Int]] = {
    val nodeIds = batchNodes.flatten.distinct
    val psGetResults = mappingMatrix.asyncPsfGet(new GetNodesMap(
      new GetNodesMapParam(mappingMatrix.id, nodeIds))).get(600000, TimeUnit.MILLISECONDS)
      .asInstanceOf[GetNodesMapResult].getResult
    val result = batchNodes.map(ele => ele.map(x => psGetResults.getLong(x).toInt))
    result.iterator
  }

  def batchGet(batchNodes: Array[(ReadOnlyByteArray, ReadOnlyByteArray)]): Iterator[(Long, Long)] = {
    val nodeIds = batchNodes.flatMap(x => Seq(x._1, x._2)).distinct
    val psGetResults = mappingMatrix.asyncPsfGet(new GetNodesMap(
      new GetNodesMapParam(mappingMatrix.id, nodeIds))).get(600000, TimeUnit.MILLISECONDS)
      .asInstanceOf[GetNodesMapResult].getResult
    val result = batchNodes.map(ele => (psGetResults.getLong(ele._1), psGetResults.getLong(ele._2)))
    result.iterator
  }

  def getValuesWithWeight(keysRDD: RDD[(ReadOnlyByteArray, ReadOnlyByteArray, Float)], batchSize: Int): RDD[(Long, Long, Float)] = {
    val results =  keysRDD.mapPartitions { iterator =>
      iterator.sliding(batchSize, batchSize)
        .map(batch => batchGetWithWeight(batch.toArray))
    }.flatMap(x => x)
    results
  }

  def batchGetWithWeight(batchNodes: Array[(ReadOnlyByteArray, ReadOnlyByteArray, Float)]): Iterator[(Long, Long, Float)] = {
    val nodeIds = batchNodes.flatMap(x => Seq(x._1, x._2)).distinct
    val psGetResults = mappingMatrix.asyncPsfGet(new GetNodesMap(
      new GetNodesMapParam(mappingMatrix.id, nodeIds))).get(600000, TimeUnit.MILLISECONDS)
      .asInstanceOf[GetNodesMapResult].getResult
    val result = batchNodes.map(ele => (psGetResults.getLong(ele._1), psGetResults.getLong(ele._2), ele._3))
    result.iterator
  }



  /**
    * Dump the embedding matrix on PS to HDFS
    *
    * @param checkpointId checkpoint id
    */
  def checkpointEmbeddingMatrix(checkpointId: Int) = {
    val matrixNames = new Array[String](1)
    matrixNames(0) = mappingMatrix.name
    CheckpointUtils.checkpoint(checkpointId, matrixNames)
  }
}

object ReIndexPSModel {
  def apply(modelContext: ModelContext): ReIndexPSModel = {
    val mappingContext: MatrixContext = new MatrixContext(modelContext.getModelName, 1, -1)
    mappingContext.setValidIndexNum(modelContext.getNodeNum)
    mappingContext.setValueType(classOf[ReIndexValue])
    mappingContext.setPartitionNum(modelContext.getPartitionNum)
    mappingContext.setRowType(RowType.T_ANY_ANYKEY_SPARSE)
    mappingContext.setPartitionerClass(classOf[HashPartitioner])
    val embeddingMatrix = PSMatrix.matrix(mappingContext)
    val model = new ReIndexPSModel(embeddingMatrix)
    model
  }
}
