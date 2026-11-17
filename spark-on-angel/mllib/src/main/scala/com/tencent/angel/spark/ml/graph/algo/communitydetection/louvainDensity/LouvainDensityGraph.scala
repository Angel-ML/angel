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
package com.tencent.angel.spark.ml.graph.algo.communitydetection.louvainDensity

import com.tencent.angel.ml.math2.vector.LongFloatVector
import com.tencent.angel.spark.ml.graph.algo.communitydetection.louvainDensity.LouvainDensityGraph.edgeTripleRDD2GraphPartitions
import org.apache.spark.{HashPartitioner, SparkPrivateClassProxy}
import org.apache.spark.graphx.PartitionStrategy.EdgePartition2D
import org.apache.spark.graphx.{Edge, Graph}
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import com.tencent.angel.ml.math2.vector.{LongFloatVector, LongLongVector}
import org.apache.spark.ml.evaluation.ClusteringEvaluator
import scala.collection.mutable.ArrayBuffer


class LouvainDensityGraph(
                           @transient val graph: RDD[LouvainDensityGraphPartition],
                           louvainPSModel: LouvainDensityPSModel,
                           globalDensity: Double) extends Serializable {

  //2m for unweighted graph ; 2*sum(edge weights) for weighted graph
  private val totalWeights: Double = {
    val total = graph.map(_.partitionWeights).sum()
    //println(s"totalNodeWeights=$total")
    total
  }

  /**
   * get the pair(community,node) in all partitions
   *
   * @param model louvain ps model
   * @return
   */
  def commId2NodeId(model: LouvainDensityPSModel): RDD[(Long, Long)] = {
    graph.flatMap(_.partComm2nodeParis(model))
  }

  /**
   * get the pair(community,node) in all partitions
   *
   * @param model louvain ps model
   * @return
   */
  def commId2NodeIdFromOutVector(model: LouvainDensityPSModel): RDD[(Long, Long)] = {
    graph.flatMap(_.partComm2nodeParisFromOutVector(model))
  }


  /**
   * check the community id is the min of node ids
   *
   * @param model louvain ps model
   * @return the wrong nodes number
   */
  def checkCommId(model: LouvainDensityPSModel): Long = {
    // check ids
    commId2NodeId(model).groupByKey.filter { case (commId, nodes) =>
      commId != nodes.min
    }.count()
  }

  /**
   * comuter the total weights for check
   *
   * @param model louvain ps model
   * @return
   */
  def checkTotalSum(model: LouvainDensityPSModel): Double = {
    // check totalSum
    distinctCommIds(model).mapPartitions { iter =>
      if (iter.nonEmpty) {
        val arr = iter.toArray
        val s = model.community2weightPSVector.pull(arr).asInstanceOf[LongFloatVector].sum()
        Iterator(s)
      } else {
        Iterator(0.0)
      }
    }.sum()
  }

  /**
   * get all distinct community ids
   *
   * @param model louvain ps model
   * @return
   */
  def distinctCommIds(model: LouvainDensityPSModel): RDD[Long] = {
    graph.flatMap(_.partCommunityIds(model)).distinct()
  }

  /**
   * save (node id ,community id) in path
   *
   * @param path     save path
   * @param nodesRDD node list
   */
  def save(path: String, nodesRDD: RDD[Long]): Unit = {
    nodesRDD.mapPartitions { iter =>
      val nodes = iter.toArray
      val pair = nodes.zip(louvainPSModel.getNode2commMap(nodes).get(nodes))
      pair.map { case (id, comm) => s"$id,$comm" }.toIterator
    }.saveAsTextFile(path)
  }

  /**
   * update node weights and community weights for each partition
   *
   * @return
   */
  def updateNodeWeightsToPS(): this.type = {
    graph.foreach(_.updateNodeWeightsToPS(louvainPSModel))
    this
  }


  /**
   * the first phase of the louvain algorithm
   *
   * @param maxIter   the max iteration number in first phase
   * @param batchSize the batch size for compute
   * @param eps       the min modularity increment value
   */
  def modularityOptimize(maxIter: Int, batchSize: Int, eps: Double = 0.0, preserveRate: Double = 0.0, useMergeStrategy: Boolean, bestModularity: Double): Unit = {
    var curTime = System.currentTimeMillis()
    //val nodeTmp = Array( 3, 4,  7l)
    //val nodeTmp = Array.range(0, 34).map(_.toLong)
    //val nodeTmp = Array( 29, 30,  31l,33,9,13,21,11,10,16)

    var deltaQ = Double.MaxValue
    var prevQ = bestModularity
    var optIter = 0
    while (optIter < maxIter && deltaQ > eps) {
      println(s"------modularityOptimize---iter $optIter-----------")
      curTime = System.currentTimeMillis()

      /*
      println("node to community befor modularityOptimize")
      println("in infor")
      louvainPSModel.getNode2commPairsArr(nodeTmp).sortBy(_._1).foreach(println)
      println("----------")
      println("out infor")
      louvainPSModel.node2CommunityOutPSVector.pull(nodeTmp.clone()).asInstanceOf[LongLongVector].get(nodeTmp).zip(nodeTmp).sortBy(_._2).map(a => (a._2, a._1)).foreach(println)

       */


      modularityOptimize(batchSize, shuffle = false, preserveRate) //optIter != 0


      /*
      println("node to community after")
      println("in infor")
      louvainPSModel.getNode2commPairsArr(nodeTmp).sortBy(_._1).foreach(println)
      println("out infor")
      louvainPSModel.node2CommunityOutPSVector.pull(nodeTmp.clone()).asInstanceOf[LongLongVector].get(nodeTmp).zip(nodeTmp).sortBy(_._2).map(a => (a._2, a._1)).foreach(println)

       */


      if (useMergeStrategy) {
        /*
        println("node to community merge befor")
        println("in infor")
        louvainPSModel.getNode2commPairsArr(nodeTmp).sortBy(_._1).foreach(println)
        println("out infor")
        louvainPSModel.node2CommunityOutPSVector.pull(nodeTmp.clone()).asInstanceOf[LongLongVector].get(nodeTmp).zip(nodeTmp).sortBy(_._2).map(a => (a._2, a._1)).foreach(println)

         */

        mergeCommunity(batchSize, checkpointInterval = 50)

        /*
        println("node to community merge after")
        println("in infor")
        louvainPSModel.getNode2commPairsArr(nodeTmp).sortBy(_._1).foreach(println)
        println("out infor")
        louvainPSModel.node2CommunityOutPSVector.pull(nodeTmp.clone()).asInstanceOf[LongLongVector].get(nodeTmp).zip(nodeTmp).sortBy(_._1).map(a => (a._2, a._1)).foreach(println)
        println("final infor")
        louvainPSModel.node2CommunityFinalPSVector.pull(nodeTmp.clone()).asInstanceOf[LongLongVector].get(nodeTmp).zip(nodeTmp).sortBy(_._1).map(a => (a._2, a._1)).foreach(println)
         */


      }

      updateCommunityInforBoth(batchSize, "out")


      println(s"opt, takes ${System.currentTimeMillis() - curTime}ms")

      curTime = System.currentTimeMillis()


      val q = getModularity("out")
      println(s" Q = $q, takes ${System.currentTimeMillis() - curTime}ms")
      curTime = System.currentTimeMillis()
      deltaQ = q - prevQ
      if (deltaQ > eps) {
        prevQ = q
        louvainPSModel.resetMsgs()
      }
      else {
        //println("=========updateCommunityInforBoth in ======")
        updateCommunityInforBoth(batchSize, "in")
        //println("=========end end ======")
        //println(getModularity("in"))
      }

      println(s"numCommunity = $getNumOfCommunity, takes ${System.currentTimeMillis() - curTime}ms")
      optIter += 1
    }
  }

  private def modularityOptimize(batchSize: Int, shuffle: Boolean, preserveRate: Double): Unit = {
    graph.foreach(_.modularityOptimize(louvainPSModel, totalWeights, batchSize, shuffle, preserveRate, globalDensity))
  }

  def getModularity(source: String): Double = {
    //println("ji suan le duo sha")
    /*
    println("start compute Modularity")
    println("-------commId2NodeId in--------")
    commId2NodeId(louvainPSModel).sortByKey().foreach(println)
    println("-------commId2NodeId out--------")
    commId2NodeIdFromOutVector(louvainPSModel).sortByKey().foreach(println)
    println("-----end-----")
     */
    val allModularityDensity = graph.flatMap(_.partCommunityIdsFromBoth(louvainPSModel, source)).distinct()
      .mapPartitionsWithIndex { (index, iter) =>
        val communities = iter.toArray
        val comm2Size = louvainPSModel.comm2SizePSVector.pull(communities.clone()).asInstanceOf[LongFloatVector].get(communities)
        val comm2NumberSelfEdge = louvainPSModel.comm2NumberSelfEdgePSVector.pull(communities.clone()).asInstanceOf[LongFloatVector].get(communities)
        val comm2weight = louvainPSModel.community2weightPSVector.pull(communities.clone()).asInstanceOf[LongFloatVector].get(communities)
        val comm2SelfWeigh = louvainPSModel.comm2SelfWeightPSVector.pull(communities.clone()).asInstanceOf[LongFloatVector].get(communities)

        /*
        println("---- compute Modularity----")
        println(communities.mkString(" "))
        println(comm2Size.mkString(" "))
        println(comm2NumberSelfEdge.mkString(" "))
        println(comm2weight.mkString(" "))
        println(comm2SelfWeigh.mkString(" "))
         */


        val partModularityDensity = communities.indices.map { i =>
          val p = if (comm2Size(i) <= 1) 0.0 else 2 * comm2NumberSelfEdge(i) / (comm2Size(i) * (comm2Size(i) - 1)).toDouble - globalDensity

          /*
          println("----p---")
          println(communities(i), p)
          println(2 * comm2SelfWeigh(i))
          println(Math.pow(comm2weight(i) * p, 2) / totalWeights)
          println(2 * comm2SelfWeigh(i) * p - Math.pow(comm2weight(i) * p, 2) / totalWeights)
           */

          2 * comm2SelfWeigh(i) * p - Math.pow(comm2weight(i) * p, 2) / totalWeights //?????*2
        }.sum
        //println("-------hahhah")
        //println(partModularityDensity / totalWeights)
        Iterator(partModularityDensity / totalWeights)
      }
    //println("aaaaaaaaaa", xxx.sum())
    allModularityDensity.sum()
  }


  /**
   * get the communities number
   *
   * @return
   */
  def getNumOfCommunity: Long = {
    this.graph.flatMap(_.node2community(louvainPSModel)).values.distinct().count()
  }

  /**
   * the second phase of louvain;build a new network of communities
   *
   * @param batchSize the compute size
   * @param storageLevel
   * @return
   */
  def folding(batchSize: Int, storageLevel: StorageLevel): LouvainDensityGraph = {

    val curTime = System.currentTimeMillis()
    val newEdges = this.graph.flatMap { part =>
      part.partFolding(louvainPSModel, batchSize)
    }.reduceByKey { (a, b) => (a._1 + b._1, a._2 + b._2) }.map { case ((src, dst), (wgt, num)) =>
      (src, dst, wgt / 2.0f, num / 2.0f)
    }

    val newGraph = edgeTripleRDD2GraphPartitions(newEdges, louvainPSModel)
    newGraph.foreachPartition(_ => Unit)
    this.graph.unpersist()
    println(s"folding, takes ${System.currentTimeMillis() - curTime}ms")
    new LouvainDensityGraph(newGraph, louvainPSModel, globalDensity)
  }

  /*
    def updateCommunityInfor(batchSize: Int): Unit = {

      val edgesInforsInCommunity = this.graph.flatMap { part => part.getEdgesInforsFromOutVector(louvainPSModel, batchSize)
      }.reduceByKey((a, b) => (a._1 + b._1, a._2 + b._2))
        .map { case (community, wgt) => (community, (wgt._1 / 2.0f, wgt._2 / 2))
        }
      edgesInforsInCommunity.foreachPartition { iter =>
        val (comm, infor) = iter.toArray.unzip
        val (selfWeigh, numberSelfEdge) = infor.unzip
        louvainPSModel.updateCommEdgesInfor(comm, selfWeigh, numberSelfEdge)
      }

      val nodesInforsInCommunity = this.graph.flatMap { part => part.getNodesInforsFromOutVector(louvainPSModel, batchSize)
      }.reduceByKey((a, b) => (a._1 + b._1, a._2 + b._2))
      nodesInforsInCommunity.foreachPartition { iter =>
        val (comm, infor) = iter.toArray.unzip
        val (weight, size) = infor.unzip
        louvainPSModel.updateCommNodesInfor(comm, weight, size)
      }

      println("------updateCommunityInfor--- edges------")
      edgesInforsInCommunity.sortByKey().foreach(println)
      println("------updateCommunityInfor---- nodes-----")
      nodesInforsInCommunity.sortByKey().foreach(println)

    }

   */

  def updateCommunityInforBoth(batchSize: Int, source: String): Unit = {

    val edgesInforsInCommunity = this.graph.flatMap { part => part.getEdgesInforsFromBothVector(louvainPSModel, batchSize, source)
    }.reduceByKey((a, b) => (a._1 + b._1, a._2 + b._2))
    //.map { case (community, wgt) => (community, (wgt._1, wgt._2))
    //}
    edgesInforsInCommunity.foreachPartition { iter =>
      val (comm, infor) = iter.toArray.unzip
      val (selfWeigh, numberSelfEdge) = infor.unzip
      louvainPSModel.updateCommEdgesInfor(comm, selfWeigh, numberSelfEdge)
    }

    val nodesInforsInCommunity = this.graph.flatMap { part => part.getNodesInforsFromBothVector(louvainPSModel, batchSize, source)
    }.reduceByKey((a, b) => (a._1 + b._1, a._2 + b._2))
    nodesInforsInCommunity.foreachPartition { iter =>
      val (comm, infor) = iter.toArray.unzip
      val (weight, size) = infor.unzip
      //println("======nodesInforsInCommunity======")
      //println(weight.sum)
      louvainPSModel.updateCommNodesInfor(comm, weight, size)
    }
    /*
        println("--------",source)
        println("------updateCommunityInfor--- edges------")
        edgesInforsInCommunity.sortByKey().foreach(println)
        println("------updateCommunityInfor---- nodes-----")
        nodesInforsInCommunity.sortByKey().foreach(println)

     */

  }


  /**
   * build a new graph with edge(cpmmunityId,nodeId);Compute the connected component
   *
   * @param checkpointInterval
   * @return
   */
  def mergeCommunity(bufferSize: Int = 1000000, checkpointInterval: Int): Unit = {
    val oriRdd = commId2NodeIdFromOutVector(louvainPSModel).map(_.swap)

    //println("node to community in  merge")
    //oriRdd.sortByKey().foreach(println)
    //println("------end-----")

    val DUMMY_VALUE = null.asInstanceOf[Byte]
    val partitionNum = oriRdd.getNumPartitions
    val edgeRdd = oriRdd.map {
      case (src, dst) =>
        val pid = EdgePartition2D.getPartition(src, dst, partitionNum)
        if (src < dst) (pid, (src, dst)) else (pid, (dst, src))
    }.partitionBy(new HashPartitioner(partitionNum))
      .map { case (_, (src, dst)) => new Edge(src, dst, DUMMY_VALUE) }

    val rawG = Graph.fromEdges(edgeRdd, 1L)
      .mapVertices { case (vid, _) => vid.toLong }
      .persist()

    var messages = rawG.aggregateMessages[Long](
      {
        t =>
          if (t.srcAttr < t.dstAttr)
            t.sendToSrc(t.dstAttr)
          if (t.dstAttr < t.srcAttr)
            t.sendToDst(t.srcAttr)
      }, math.max
    ).persist()
    var activeMessages = messages.count()
    var i = 0
    var g = rawG
    val maxiter = 30
    while (i < maxiter && activeMessages > 0) {
      println(s"+++ merge community iter: $i, activeMsg: $activeMessages")

      i = i + 1
      val prevG = g

      g = g.outerJoinVertices[Long, Long](messages) {
        case (_, data, Some(update)) => Math.max(data, update)
        case (_, data, None) => data
      }.persist()
      if (i % checkpointInterval == 0) {
        g.checkpoint()
      }
      val oldMessage = messages
      messages = g.aggregateMessages[Long](
        {
          t =>
            if (t.srcAttr < t.dstAttr)
              t.sendToSrc(t.dstAttr)
            if (t.dstAttr < t.srcAttr)
              t.sendToDst(t.srcAttr)
        }, math.max
      ).persist()
      activeMessages = messages.count()
      prevG.unpersistVertices()
      prevG.edges.unpersist()
      oldMessage.unpersist()
    }
    messages.unpersist()

    val result = g.vertices.map(e => (e._1, e._2)).persist()

    result.count()
    g.unpersistVertices()
    g.edges.unpersist()

    /*
    println("********")
    result.sortByKey().foreach(println)
    println("********")
    commId2NodeIdFromOutVector(louvainPSModel).map(_.swap).sortByKey().foreach(println)
     */

    oriRdd.leftOuterJoin(result).map {
      case (vId, (oldComm, newCommOpt)) => (vId, oldComm, newCommOpt.getOrElse(oldComm))
    }.foreachPartition { iter =>
      if (iter.nonEmpty) {
        val newCommIdsBuffer = new ArrayBuffer[Long]()
        val nodesBuffer = new ArrayBuffer[Long]()

        while (iter.hasNext) {
          var used = 0
          while (iter.hasNext && used < bufferSize) {
            val (vId, oldComm, newComm) = iter.next
            if (oldComm != newComm) {
              newCommIdsBuffer += newComm
              nodesBuffer += vId
              used += 1
            }
          }
          if (used > 0) {
            // update node2comm
            //println("updateNode2communityOut")
            //println(nodesBuffer.toArray.mkString(","))
            //println(newCommIdsBuffer.toArray.mkString(","))

            louvainPSModel.updateNode2communityOut(nodesBuffer.toArray, newCommIdsBuffer.toArray)
            nodesBuffer.clear()
            newCommIdsBuffer.clear()
          }
        }
      }
    }
    /*
    val edgesInforsInCommunity = this.graph.flatMap { part => part.edgesInforsInCommunity(louvainPSModel, bufferSize)
    }.reduceByKey((a, b) => (a._1 + b._1, a._2 + b._2))
      .map { case (community, wgt) => (community, (wgt._1 / 2.0f, wgt._2 / 2))
      }

    old2NewCommRDD.distinct().map(_.swap).join(edgesInforsInCommunity)
      .foreachPartition { iter =>
        if (iter.nonEmpty) {
          val (newCommIds, tmp1) = iter.toArray.unzip
          val (oldCommIds, tmp2) = tmp1.unzip
          val (selfWeight, selfNumber) = tmp2.unzip

          val (oldCommunity2weight, oldComm2NumberSelfEdge, oldComm2Size, oldComm2SelfWeight) = louvainPSModel.getCommAllInfor(oldCommIds)
          val oldCommunityInforDelta = SparkPrivateClassProxy.createOpenHashMap[Long, (Float, Float, Float, Float)]()
          val newCommunityInforDelta = SparkPrivateClassProxy.createOpenHashMap[Long, (Float, Float, Float, Float)]()

          oldCommIds.indices.foreach { i =>
            val newCommunity = newCommIds(i)
            val oldCommunity = oldCommIds(i)
            val oldWeight = oldCommunity2weight.get(oldCommunity)
            val oldNumberSelfEdge = oldComm2NumberSelfEdge.get(oldCommunity)
            val oldSize = oldComm2Size.get(oldCommunity)
            val oldSelfWeight = oldComm2SelfWeight.get(oldCommunity)
            oldCommunityInforDelta.changeValue(oldCommunity, (-oldWeight, -oldSelfWeight, -oldSize, -oldNumberSelfEdge), { a => (a._1 - oldWeight, a._2 - oldSelfWeight, a._3 - oldSize, a._4 - oldNumberSelfEdge) })
            newCommunityInforDelta.changeValue(newCommunity, (oldWeight, selfWeight(i), oldSize, selfNumber(i)), { a => (a._1 + oldWeight, selfWeight(i), a._3 + oldSize, selfNumber(i)) })
          }
          val (comm1, communityInfor1) = oldCommunityInforDelta.unzip
          louvainPSModel.incrementCommInfor(comm1.toArray, communityInfor1.toArray)

          val (comm2, communityInfor2) = newCommunityInforDelta.unzip
          louvainPSModel.incrementAndUpdateCommInfor(comm2.toArray, communityInfor2.toArray)
        }
      }

     */
  }
}


object LouvainDensityGraph {

  /**
   * transform edges rdd to louvain graph partition
   *
   * @param tripleRdd    the edges triple RDD
   * @param model        louvain ps model
   * @param numPartition the rdd partition number
   * @param storageLevel
   * @return
   */
  def edgeTripleRDD2GraphPartitions(tripleRdd: RDD[(Long, Long, Float, Float)],
                                    model: LouvainDensityPSModel = null,
                                    numPartition: Option[Int] = None,
                                    storageLevel: StorageLevel = StorageLevel.MEMORY_ONLY)
  : RDD[LouvainDensityGraphPartition] = {

    /*
        println("=====start edgeTripleRDD2GraphPartitions")
        tripleRdd.map(a => (a._1, a._2, a._3)).foreach(a => println(a.toString()))
        println("=======end===")

     */

    val partNum = numPartition.getOrElse(tripleRdd.getNumPartitions)
    tripleRdd.flatMap { case (src, dst, wgt, num) =>
      Iterator((src, (dst, wgt, num)), (dst, (src, wgt, num)))
    }.groupByKey(partNum).mapPartitions { iter =>
      if (iter.nonEmpty) {
        val keys = new ArrayBuffer[Long]()
        val neighbors = new ArrayBuffer[Array[Long]]()
        val weights = new ArrayBuffer[Array[Float]]()
        val edgesNumber = new ArrayBuffer[Array[Float]]()
        iter.foreach { case (key, group) =>
          keys += key
          val (e, w, n) = group.unzip3
          neighbors += e.toArray
          weights += w.toArray
          edgesNumber += n.toArray
        }
        Iterator.single((keys.toArray, neighbors.toArray, weights.toArray, edgesNumber.toArray))
      } else {
        Iterator.empty
      }
    }.map { case (keys, neighbors, weights, edgesNumber) =>
      // calc nodeWeights
      //(community2weight,comm2NumberSelfEdge,comm2Size,comm2SelfWeight)
      val (nodeWeights, nodeNumberSelfEdge, nodeSize, nodeSelfWeight) = if (null != model) {
        val allInfor = model.getCommAllInfor(keys)
        (allInfor._1.get(keys), allInfor._2.get(keys), allInfor._3.get(keys), allInfor._4.get(keys))

      } else {
        val length = keys.length
        (weights.map(_.sum), Array.fill(length)(0f), Array.fill(length)(1f), Array.fill(length)(0f))
      }

      /*
            println("--------build graph------------")
            println(keys.mkString(","))
            println("------")
            keys.zip(neighbors).foreach { a => println(a._1, a._2.mkString(" ")) }
            println("------")
            keys.zip(weights).foreach { a => println(a._1, a._2.mkString(" ")) }
            println("------")
            keys.zip(edgesNumber).foreach { a => println(a._1, a._2.mkString(" ")) }
            println("------")
            keys.zip(nodeWeights).foreach { a => print(a._1, a._2) }
            println("----nodeNumberSelfEdge--")
            keys.zip(nodeNumberSelfEdge).foreach { a => print(a._1, a._2) }
            println("------")
            keys.zip(nodeSize).foreach { a => print(a._1, a._2) }
            println("------")
            keys.zip(nodeSelfWeight).foreach { a => print(a._1, a._2) }

       */


      new LouvainDensityGraphPartition(keys, neighbors, weights, edgesNumber, nodeWeights, nodeNumberSelfEdge, nodeSize, nodeSelfWeight)
    }.persist(storageLevel)
  }
}

