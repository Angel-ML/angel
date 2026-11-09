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

package com.tencent.angel.spark.ml.graph.algo.embedding.embeddingpooing

import java.text.SimpleDateFormat
import java.util.Date
import java.util.concurrent.TimeUnit

import com.tencent.angel.ml.matrix.{MatrixContext, RowType}
import com.tencent.angel.spark.ml.util.LogUtils
import com.tencent.angel.spark.models.PSMatrix
import it.unimi.dsi.fastutil.ints.{Int2ObjectOpenHashMap, IntOpenHashSet}
import org.apache.hadoop.fs.Path
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

import scala.collection.mutable
import scala.util.Random

class EmbeddingPoolingModel(numNode: Int,
                            embeddingDim: Int,
                            numPsParts: Int,
                            bathSize: Int,
                            outputPath: String) extends Serializable {
  val matrixName = "embedding"
  // Create one ps matrix to hold the embeddings
  val mc: MatrixContext = new MatrixContext(matrixName, 1, numNode)
  mc.setMaxRowNumInBlock(1)
  mc.setMaxColNumInBlock(numNode / numPsParts)
  mc.setRowType(RowType.T_ANY_INTKEY_DENSE)
  mc.setValueType(classOf[EmbeddingNode])
  mc.setInitFunc(new EmbeddingInitFunc(embeddingDim))
  val psMatrix: PSMatrix = PSMatrix.matrix(mc)
  val matrixId: Int = psMatrix.id
  var totalPullTime: Long = 0
  var totalPushTime: Long = 0
  var totalMakeParamTime: Long = 0
  var totalCalTime: Long = 0
  var totalMakeGradTime: Long = 0
  var totalCallNum: Long = 0
  var totalWaitPullTime: Long = 0

  // initialize embeddings
  def initialize(embeddingsRDD: RDD[(Int, Array[Float])], params: EmbeddingPoolingParam): Unit = {
    randomInitialize(Random.nextInt())
    val beforeInitialize = System.currentTimeMillis()
    embeddingsRDD.mapPartitions { iterator =>
      iterator.sliding(params.batchSize, params.batchSize)
        .map(batch => extraUpdate(batch.toArray))
    }.count()
    LogUtils.logTime(s"Model successfully extra Initial, cost ${(System.currentTimeMillis() - beforeInitialize) / 1000.0}s")
  }

  def extraUpdate(strings: Array[(Int, Array[Float])]): Unit = {
    val inputUpdates = new Int2ObjectOpenHashMap[Array[Float]]()
    strings.map{ele =>
      inputUpdates.put(ele._1, ele._2)
    }
    psMatrix.psfUpdate(new EmbeddingAdjust(new EmbeddingAdjustParam(matrixId, inputUpdates))).get()
  }

  def randomInitialize(seed: Int): Unit = {
    val beforeRandomize = System.currentTimeMillis()
    psMatrix.psfUpdate(new EmbeddingPoolingModelRandomize(new RandomizeUpdateParam(matrixId, embeddingDim, seed))).get()
    logTime(s"Model successfully Randomized, cost ${(System.currentTimeMillis() - beforeRandomize) / 1000.0}s")
  }

  def this(params: EmbeddingPoolingParam) {
    this(params.maxIndex, params.embeddingDim, params.numPsPartitions, params.batchSize, params.outputPath)
  }

  def logTime(msg: String): Unit = {
    val time = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date)
    println(s"[$time] $msg")
  }

  def fit(sequences: RDD[(String, Array[Int], Array[Float])], params: EmbeddingPoolingParam): RDD[(String, Array[Float])] = {
    val result = sequences.mapPartitions {
      iterator =>
        iterator.sliding(params.batchSize, params.batchSize)
          .map(batch => optimize(batch.toArray))
    }.flatMap(x => x)
    result
  }

  def optimize(batch: Array[(String, Array[Int], Array[Float])]): Iterator[(String, Array[Float])] = {
    val distinctWords = new IntOpenHashSet()
    val batchIterator = batch.iterator
    while (batchIterator.hasNext) {
      val ele = batchIterator.next()
      val words = ele._2
      words.map(x => distinctWords.add(x))
    }
    val embeddings: Int2ObjectOpenHashMap[Array[Float]] = psMatrix
      .asyncPsfGet(new EmbeddingGet(new EmbeddingGetParam(matrixId, distinctWords.toIntArray)))
      .get(1800000, TimeUnit.MILLISECONDS).asInstanceOf[EmbeddingGetResult].getResult
    val results = new mutable.HashMap[String, Array[Float]]()
    val poolingResult = new Array[Float](embeddingDim)
    for(ele <- batch) {
      val uid = ele._1
      val words = ele._2
      val weights = ele._3
      for(i <- words.indices) {
        val tmp = embeddings.get(words(i)).map(x => x * weights(i))
        poolingResult.indices.foreach(i => poolingResult(i) += tmp(i))
      }
      results.put(uid, poolingResult)
    }
    results.toArray.iterator
  }

  def save(output: String, result: RDD[(String, Array[Float])]): Unit ={
    val ss = SparkSession.builder().getOrCreate()
    deleteIfExists(output, ss)
    result.map(x => x._1 + "\t" + x._2.mkString(" ")).saveAsTextFile(output)
  }

  def deleteIfExists(output: String, ss: SparkSession): Unit = {
    val path = new Path(output)
    val fs = path.getFileSystem(ss.sparkContext.hadoopConfiguration)
    if (fs.exists(path)) {
      fs.delete(path, true)
    }
  }
}
