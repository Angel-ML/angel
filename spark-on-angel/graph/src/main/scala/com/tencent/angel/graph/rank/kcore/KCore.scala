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

package com.tencent.angel.graph.rank.kcore

import com.tencent.angel.graph.common.param.ModelContext
import com.tencent.angel.graph.utils.io.Log
import com.tencent.angel.graph.utils.{BatchIter, GraphIO, Stats}
import com.tencent.angel.spark.context.PSContext
import com.tencent.angel.graph.utils.params._
import com.tencent.angel.ml.math2.VFactory
import org.apache.spark.SparkContext
import org.apache.spark.ml.Transformer
import org.apache.spark.ml.param.ParamMap
import org.apache.spark.ml.util.Identifiable
import org.apache.spark.sql.types.{IntegerType, LongType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Dataset, Row}
import org.apache.spark.rdd.RDD

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

/**
  * KCore algorithm implementation
  *
  * @param uid
  */

class KCore(override val uid: String) extends Transformer
  with HasSrcNodeIdCol with HasDstNodeIdCol with HasOutputNodeIdCol with HasOutputCoreIdCol
  with HasStorageLevel with HasPartitionNum with HasPSPartitionNum with HasUseBalancePartition
  with HasBalancePartitionPercent with HasBatchSize with HasPullBatchSize with HasBufferSize
  with HasSetCheckPoint with HasExecMode with HasNeedReplicaEdge with HasTopK {
  
  def this() = this(Identifiable.randomUID(s"KCore"))
  
  var index: RDD[Long] = _
  var initialCore: RDD[(Long, Int)] = _
  def setInitialCore(init: RDD[(Long, Int)]): Unit = { this.initialCore = init }
  var tempOutputDir: String = _
  def setTempOutput(in: String): Unit = { this.tempOutputDir = in }
  
  var staticCores: RDD[Long] = _
  def setStaticCores(data: RDD[Long]): Unit = { this.staticCores = data }
  var outputDir: String = _
  def setOutput(in: String): Unit = { this.outputDir = in }
  
  override def transform(dataset: Dataset[_]): DataFrame = {
    val edges = GraphIO.loadEdgesFromDF(dataset, $(srcNodeIdCol), $(dstNodeIdCol))
    edges.persist($(storageLevel))
  
    val (minId, maxId, numEdges) = Stats.summarize(edges)
    Log.withTimePrintln(s"minId=$minId maxId=$maxId numEdges=$numEdges level=${$(storageLevel)}")
    
    // Start PS and init the model
    Log.withTimePrintln("start to run ps")
    PSContext.getOrCreate(SparkContext.getOrCreate())
    val modelContext = new ModelContext($(psPartitionNum), minId, maxId, -1,
      "cores",  SparkContext.getOrCreate().hadoopConfiguration)
    val model = KCorePSModel(modelContext, edges, $(execMode), $(useBalancePartition), $(balancePartitionPercent))
  
    var graph =
      if ($(execMode) == "dense") {
        makeDenseGraph(edges, model)
      }
      else if ($(execMode) == "sparse") {
        makeSparseGraph(edges, model)
      }
      else if ($(execMode) == "mid") {
        makeMidGraph(edges, model)
      }
      else {
        makeFullGraph(edges)
      }
    edges.unpersist(blocking = false)
    
    //    model.resetCores()
    if (initialCore != null)
      initialCore.foreachPartition(iter => KCore.pushInitCore(iter, model))
    else {
      graph.foreach(_.initCores(model, ${batchSize}))
    }
    
    if ($(setCheckPoint)) {
      val cp0StartTime = System.currentTimeMillis()
      model.checkpoint(0)
      Log.withTimePrintln(s"iteration: 0, checkpoint, cost: ${System.currentTimeMillis() - cp0StartTime} ms")
    }
    
    val numNodes = graph.map(_.numNodes()).sum()
    val numEdges_ = graph.map(_.numEdges()).sum()
    Log.withTimePrintln(s"curIteration=0, numCores=numNodes=$numNodes, numEdges=$numEdges_")
    
    var curIteration = 1
    var numMsgs = numNodes.toLong
    var totalUpdates = 0L
    
    var temp = graph
    var preMinCore = if ($(execMode) == "sparse" || $(execMode) == "full") 1 else $(topK)
    var curMinCore = preMinCore
    var compress = if ($(execMode) == "dense" || $(execMode) == "mid") true else false
    var numKeys2calc = 0L
    val numFirsts = if (initialCore == null) ${bufferSize} else 1
    val startIterationTime = System.currentTimeMillis()
    do {
      if (curIteration <= numFirsts) {
        val t = temp.map(_.firstIterations(model, ${pullBatchSize}, curIteration, numFirsts, preMinCore)).reduce((a, b) => (a._1 min b._1, a._2 + b._2))
        curMinCore = t._1
        numMsgs = t._2
        totalUpdates += numMsgs
      } else {
        val t = temp.map(_.nextIterationsByBatch(model, ${pullBatchSize}, curIteration, preMinCore)).reduce((a, b) => (a._1 min b._1, a._2 + b._2))
        curMinCore = t._1
        numMsgs = t._2
        totalUpdates += numMsgs
      }
      model.resetMsgs()
      numKeys2calc = model.numMsgs()
      Log.withTimePrintln(s"curIteration=$curIteration numMsgs=$numMsgs preMinCore=$preMinCore curMinCore=$curMinCore numKeys2calc=$numKeys2calc")
      if (curMinCore != preMinCore) preMinCore = curMinCore
      
      if (compress && curMinCore > ${topK}) {
        //        val tempRe = graph.flatMap(_.save(model, ${pullBatchSize})).sortBy(_._2, false)
        //        val tempReDF = dataset.sparkSession.createDataFrame(tempRe.map(x => Row(x._1, x._2)), transformSchema(dataset.schema))
        //        GraphIO.save(tempReDF, tempOutputDir)
        //        println(s"saved temp result to $tempOutputDir.")
        // compress
        Log.withTimePrintln(s"compress...")
        val startCompressTime = System.currentTimeMillis()
        graph = temp.map(_.compress(model, ${pullBatchSize})).persist(${storageLevel})
        graph.count()
        val endCompressTime = System.currentTimeMillis()
        Log.withTimePrintln(s"compress cost: ${endCompressTime - startCompressTime} ms.")
        Log.withTimePrintln(s"gen kcore sub graph cost: ${endCompressTime - startIterationTime} ms")
        temp.unpersist(blocking = false)
        temp = graph.filter(_.numNodes() > 0)
        val numNodes_ = temp.map(_.numNodes()).reduce(_ + _)
        val numEdges_ = temp.map(_.numEdges()).reduce(_ + _)
        Log.withTimePrintln(s"compress2 success, curIteration=$curIteration, numNodes_=$numNodes_, numEdges_=$numEdges_")
        //        model.checkpoint(curIteration)
        compress = false
      }
      
      if ($(setCheckPoint) && (curIteration % 50 == 0)) {
        val startCPTime = System.currentTimeMillis()
        model.checkpoint(curIteration)
        Log.withTimePrintln(s"iteration: $curIteration, checkpoint, cost: ${System.currentTimeMillis() - startCPTime} ms")
      }
      curIteration += 1
    } while (numMsgs > 0)
    val endIterationTime = System.currentTimeMillis()
    
    Log.withTimePrintln(s"iteration cost total: ${endIterationTime - startIterationTime}")
    Log.withTimePrintln(s"num iterations: $curIteration, total updates: $totalUpdates")
    
    val retRdd = graph.flatMap(_.save(model, ${pullBatchSize})).sortBy(_._2, false)
    Log.withTimePrintln(s"numSavedNodes: ${retRdd.count()}")
    
    //    dataset.sparkSession.createDataFrame(retRDD, transformSchema(dataset.schema))
    dataset.sparkSession.createDataFrame(retRdd.map(x => Row(x._1, x._2)), transformSchema(dataset.schema))
  }
  
  override def transformSchema(schema: StructType): StructType = {
    StructType(Seq(
      StructField(s"${$(outputNodeIdCol)}", LongType, nullable = false),
      StructField(s"${$(outputCoreIdCol)}", IntegerType, nullable = false)
    ))
  }
  
  override def copy(extra: ParamMap): Transformer = defaultCopy(extra)
  
  def makeSparseGraph(edges: RDD[(Long, Long)], model: KCorePSModel): RDD[KCorePartition] = {
    //push static nodes and cores to ps
    assert(staticCores != null, s"error: staticCores is null!")
    val ts1 = System.currentTimeMillis()
    staticCores.foreachPartition { iter => Iterator.single(KCore.initStaticCores(iter, model))}
    println(s"push static cores cost: ${System.currentTimeMillis() - ts1} ms")
    
    // filter nodes that are needed to be calculated
    val startMkTableTime = System.currentTimeMillis()
    val neighborTable = KCore.edges2NeighborTable($(execMode), edges, ${partitionNum}, ${topK}, model, ${pullBatchSize}, $(needReplicaEdge)).persist($(storageLevel))
    val endMkTableTime = System.currentTimeMillis()
    println(s"make neighbor table cost: ${endMkTableTime - startMkTableTime} ms.")
    
    // filter static cores, nodes with degree=1 or nodes that are only connected to static nodes
    neighborTable.foreachPartition { iter =>
      val msgs = VFactory.sparseLongKeyIntVector(model.dim)
      while (iter.hasNext) {
        val (src, neis, numStatics) = iter.next()
        if (neis.isEmpty) {
          assert(numStatics > 0)
          msgs.set(src, numStatics)
        }else if (neis.length == 1 && numStatics == 0) {
          msgs.set(src, 1)
        }
      }
      model.initCores(msgs)
      println(s"2nd static cores, init ${msgs.size().toInt} cores.")
    }
    val graph = neighborTable.mapPartitionsWithIndex { case (index, it) =>
      Iterator(KCorePartition.applySparse(index, it, ${pullBatchSize}, ${topK}))}
    graph.persist($(storageLevel))
    graph.foreachPartition(_ => Unit)
    println(s"graph parts: ${graph.count()}")
    neighborTable.unpersist(blocking = false)
    graph
  }
  
  def makeDenseGraph(edges: RDD[(Long, Long)], model: KCorePSModel): RDD[KCorePartition] = {
    val startMkTableTime = System.currentTimeMillis()
    val neighborTable = KCore.edges2NeighborTable($(execMode), edges, $(partitionNum), $(topK), $(needReplicaEdge))
      .persist($(storageLevel))
    neighborTable.count()
    val endMkTableTime = System.currentTimeMillis()
    println(s"make neighbor table cost: ${endMkTableTime - startMkTableTime} ms.")
    
    neighborTable.mapPartitionsWithIndex { case (index, iter) => KCore.initModel(index, iter, model, ${batchSize})}.count()
    val graph = neighborTable.mapPartitionsWithIndex { case (index, it) =>
      Iterator(KCorePartition.applyDense(index, it, model, ${pullBatchSize}, ${topK}))}
    graph.persist($(storageLevel))
    graph.foreachPartition(_ => Unit)
    graph.count()
    neighborTable.unpersist(blocking = false)
    graph
  }
  
  def makeFullGraph(edges: RDD[(Long, Long)]): RDD[KCorePartition]  = {
    val startMkTableTime = System.currentTimeMillis()
    val neighborTable = KCore.edges2NeighborTable($(execMode), edges, $(partitionNum), $(topK), $(needReplicaEdge))
      .persist($(storageLevel))
    neighborTable.count()
    val endMkTableTime = System.currentTimeMillis()
    println(s"make neighbor table cost: ${endMkTableTime - startMkTableTime} ms.")
    
    val graph = neighborTable.mapPartitionsWithIndex((index, it) =>
      Iterator(KCorePartition.applyFull(index, it, $(batchSize))))
    graph.persist($(storageLevel))
    graph.foreachPartition(_ => Unit)
    graph.count()
    neighborTable.unpersist(blocking = false)
    graph
  }
  
  def makeMidGraph(edges: RDD[(Long, Long)], model: KCorePSModel): RDD[KCorePartition]  = {
    //push static nodes and cores to ps
    assert(staticCores != null, s"error: staticCores is null!")
    val ts1 = System.currentTimeMillis()
    staticCores.foreachPartition { iter => Iterator.single(KCore.initStaticCores(iter, model))}
    println(s"push static cores cost: ${System.currentTimeMillis() - ts1} ms")
    
    // filter nodes that are needed to be calculated
    val startMkTableTime = System.currentTimeMillis()
    val neighborTable = KCore.edges2NeighborTable($(execMode), edges, ${partitionNum}, ${topK}, model, ${pullBatchSize}, $(needReplicaEdge)).persist($(storageLevel))
    neighborTable.count()
    val endMkTableTime = System.currentTimeMillis()
    println(s"make neighbor table cost: ${endMkTableTime - startMkTableTime} ms.")
    
    neighborTable.mapPartitionsWithIndex { case (index, iter) => KCore.initModel2(index, iter, model, ${batchSize})}.count()
    
    val graph = neighborTable.mapPartitionsWithIndex { case (index, it) =>
      Iterator(KCorePartition.applyMid(index, it, model, ${pullBatchSize}, ${topK}))}
    graph.persist($(storageLevel))
    graph.foreachPartition(_ => Unit)
    graph.count()
    neighborTable.unpersist(blocking = false)
    graph
  }
  
}

object KCore {
  def edges2NeighborTable(mode: String, edges: RDD[(Long, Long)], partitionNum: Int, degreeThreshold: Int = -1,
                          needReplicaEdge: Boolean): RDD[(Long, Array[Long])] = {
    assert(mode.toLowerCase == "dense" || mode.toLowerCase == "full")
    val threshold = if (mode.toLowerCase == "dense") degreeThreshold else -1
    val newEdges = if (needReplicaEdge) edges.flatMap(f => Iterator((f._2, f._1), (f._1, f._2))) else edges
    newEdges.groupByKey(partitionNum).mapPartitions { iter =>
      if (iter.nonEmpty) {
        iter.flatMap { case (src, group) =>
          val neis = group.toArray.distinct
          if (neis.length > threshold) Iterator.single(src, neis)
          else Iterator.empty
        }
      } else Iterator.empty
    }
    
  }
  
  def edges2NeighborTable(mode: String, edges: RDD[(Long, Long)], partitionNum: Int, degreeThreshold: Int,
                          model: KCorePSModel, batchSize: Int, needReplicaEdge: Boolean): RDD[(Long, Array[Long], Int)] = {
    assert(mode.toLowerCase == "sparse" || mode.toLowerCase == "mid")
    val threshold = if (mode.toLowerCase == "mid") degreeThreshold else -1
    val newEdges = if (needReplicaEdge) edges.flatMap(f => Iterator((f._2, f._1), (f._1, f._2))) else edges
    newEdges.groupByKey(partitionNum).mapPartitions { iter =>
      BatchIter(iter, batchSize).flatMap { batchIter =>
        val nodes2Pull = new mutable.HashSet[Long]()
        batchIter.foreach { case (src, neighbors) =>
          nodes2Pull ++= neighbors
          nodes2Pull.add(src)
        }
        val pulled = model.readStaticCores(nodes2Pull.toArray)
        batchIter.flatMap { case (src, nbrs) =>
          if (pulled.get(src) > 0)
            Iterator.empty
          else {
            val nbrsArray = nbrs.toArray.distinct
            if (nbrsArray.length > threshold) {
              var numStaticNeis = 0
              val restNeis = new ArrayBuffer[Long]()
              nbrsArray.foreach { nbr =>
                if (pulled.get(nbr) > 0) numStaticNeis += 1 else restNeis.append(nbr)
              }
              Iterator.single(src, restNeis.toArray, numStaticNeis)
            }
            else {
              Iterator.empty
            }
          }
        }
      }
    }
  }
  
  def initModel(index: Int, iterator: Iterator[(Long, Array[Long])], model: KCorePSModel, batchSize: Int): Iterator[Int] = {
    iterator.sliding(batchSize, batchSize).map(pairs => initMsgs(pairs.map(_._1), model))
  }
  
  def initModel2(index: Int, iterator: Iterator[(Long, Array[Long], Int)], model: KCorePSModel, batchSize: Int): Iterator[Int] = {
    iterator.sliding(batchSize, batchSize).map(pairs => initMsgs(pairs.map(_._1), model))
  }
  
  def initMsgs(nodes: Seq[Long], model: KCorePSModel): Int = {
    val len = nodes.length
    val msgs = VFactory.sparseLongKeyIntVector(len, nodes.toArray, Array.fill(len)(1))
    model.writeInMsgs(msgs)
    Log.withTimePrintln(s"init ${nodes.length} cores.")
    msgs.clear()
    len
  }
  
  def pushInitCore(iterator: Iterator[(Long, Int)], model: KCorePSModel): Unit = {
    val (nodes, cores) = iterator.toArray.unzip
    val len = nodes.length
    val msgs = VFactory.sparseLongKeyIntVector(len, nodes, cores)
    model.initCores(msgs)
    Log.withTimePrintln(s"2nd init $len cores.")
    msgs.clear()
  }
  
  def initStaticCores(iter: Iterator[Long], model: KCorePSModel): Unit = {
    val nodes = iter.toArray
    val msgs = VFactory.sparseLongKeyIntVector(nodes.length, nodes, Array.fill(nodes.length)(1))
    model.initStaticCores(msgs)
    Log.withTimePrintln(s"init ${msgs.size()} static cores.")
    msgs.clear()
  }
  
}