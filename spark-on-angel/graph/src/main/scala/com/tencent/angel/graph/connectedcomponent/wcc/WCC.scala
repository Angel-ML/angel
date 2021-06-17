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

package com.tencent.angel.graph.connectedcomponent.wcc

import com.tencent.angel.graph.common.param.ModelContext
import com.tencent.angel.graph.utils.{GraphIO, Stats}
import com.tencent.angel.graph.utils.params._
import com.tencent.angel.spark.context.PSContext
import org.apache.spark.SparkContext
import org.apache.spark.ml.Transformer
import org.apache.spark.ml.param.{IntParam, ParamMap}
import org.apache.spark.ml.util.Identifiable
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}

class WCC(override val uid: String) extends Transformer
  with HasWeightCol with HasSrcNodeIdCol with HasDstNodeIdCol
  with HasOutputNodeIdCol with HasOutputCoreIdCol with HasBalancePartitionPercent
  with HasIsWeighted with HasPartitionNum with HasPSPartitionNum
  with HasStorageLevel with HasBatchSize with HasPullBatchSize
  with HasBufferSize with HasUseBalancePartition with HasNeedReplicaEdge {
  
  final val compressIterNum = new IntParam(this, "compressIterNum", "compressIterNum")
  final val localLimit = new IntParam(this, "localLimit", "localLimit")
  final def setCompressIterNum(num: Int): this.type = set(compressIterNum, num)
  final def setLocalLimit(num: Int): this.type = set(localLimit, num)
  setDefault(compressIterNum, 3)
  setDefault(localLimit, 100000000)
  
  def this() = this(Identifiable.randomUID("WCC-compress_ver"))
  
  override def transform(dataset: Dataset[_]): DataFrame = {
    // read edges
    val edges = GraphIO.loadEdgesFromDF(dataset, $(srcNodeIdCol), $(dstNodeIdCol), $(needReplicaEdge))
    
    edges.persist($(storageLevel))
    val numEdges = edges.count()
    
    val ss = SparkSession.builder().getOrCreate()
    var retRDD: RDD[Row] = null
    if (numEdges < $(localLimit)) {
      println(s"edgeNum less than limit, turn to local computation")
      val t1 = System.currentTimeMillis()
      val localEdgeArray = edges.collect()
      val localWCC = LocalWCC.process(localEdgeArray)
      println("localWCC cost time "+(System.currentTimeMillis() - t1)*1.0f / 1000)
      retRDD = ss.sparkContext.makeRDD(localWCC.toSeq, $(partitionNum))
        .map(f => Row.fromSeq(Seq[Any](f._1, f._2)))
      
      edges.unpersist(blocking = false)
    }
    else {
      val (minId, maxId, numEdges) = Stats.summarize(edges)
      
      println(s"minId=$minId maxId=$maxId numEdges=$numEdges level=${$(storageLevel)}")
      
      // Start PS and init the model
      println("start to run ps")
      PSContext.getOrCreate(SparkContext.getOrCreate())
      
      val modelContext = new ModelContext($(psPartitionNum), minId, maxId, -1,
        "labels",  SparkContext.getOrCreate().hadoopConfiguration)
      val model = WCCPSModel(modelContext, edges, $(useBalancePartition), $(balancePartitionPercent))
      
      // make un-directed graph, for wcc
      var graph = edges.groupByKey($(partitionNum))
        .mapPartitionsWithIndex((index, adjTable) => Iterator((0, WCCPartition.apply(index, adjTable))))
      graph.persist($(storageLevel))
      graph.foreachPartition(_ => Unit)
      graph.count()
      
      graph.foreach(_._2.initMsgs(model, $(batchSize)))
      graph.foreachPartition(_ => Unit)
      graph.count()
      
      edges.unpersist(blocking = false)
      
      var numMsgs = model.numMsgs()
      var curIteration = 0
      var prev = graph
      println(s"numMsgs=$numMsgs")
      
      // while procession on ps,
      // each node change its label into the min id of its neighbors (including itself).
      var changedCnt = 0
      var edgeAfterCP = 0L
      var compressCnt = 1
      do{
        do {
          curIteration += 1
          changedCnt = 0
          graph = prev.map(_._2.process(model, $(pullBatchSize), numMsgs, curIteration == 1))
          graph.persist($(storageLevel))
          graph.count()
          changedCnt = graph.map(_._1).reduce((n1, n2) => n1 + n2)
          prev.unpersist(blocking = false)
          prev = graph
          // model.resetMsgs()
          
          println(s"curIteration=$curIteration numMsgs=$changedCnt")
        } while ((changedCnt > 0) && (curIteration < ($(compressIterNum) * compressCnt)))
        
        compressCnt += 1
        edgeAfterCP = graph.map(_._2.edgesIfCompress(model)).reduce((n1, n2) => n1 + n2)
        println(s"rude estimate edge-num after $curIteration iters on ps: $edgeAfterCP")
        
      } while(edgeAfterCP > $(localLimit))
      
      println(s"finish ${$(compressIterNum) * (compressCnt-1)} of iters on ps, into local mode")
      
      val localEdgeArray = graph.map(_._2.compressEdges(model))
        .reduce((Arr1, Arr2) => Arr1 ++ Arr2).toArray
      println(s"local edge num: ${localEdgeArray.length}")
      
      println(s"graph compressed, turn to local computation")
      val t1 = System.currentTimeMillis()
      val localWCC = LocalWCC.process(localEdgeArray)
      println("localWCC cost time "+(System.currentTimeMillis() - t1)*1.0f / 1000)
      val localRDD = ss.sparkContext.makeRDD(localWCC.toSeq, $(partitionNum))
      
      retRDD = graph.map(_._2.save()).flatMap(f => f._2.zip(f._1))
        .leftOuterJoin(localRDD).map { case (prevlb, (nd, curlb)) =>
        curlb match {
          case Some(curlb) => (nd, curlb)
          case None => (nd, prevlb)
        }
      }.map(f => Row.fromSeq(Seq[Any](f._1, f._2)))
      PSContext.stop()
    }
    
    dataset.sparkSession.createDataFrame(retRDD, transformSchema(dataset.schema))
  }
  
  override def transformSchema(schema: StructType): StructType = {
    StructType(Seq(
      StructField(s"${$(outputNodeIdCol)}", LongType, nullable = false),
      StructField(s"${$(outputCoreIdCol)}", LongType, nullable = false)
    ))
  }
  
  override def copy(extra: ParamMap): Transformer = defaultCopy(extra)
  
}
