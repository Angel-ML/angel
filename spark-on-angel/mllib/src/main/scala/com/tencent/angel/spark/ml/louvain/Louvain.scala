package com.tencent.angel.spark.ml.louvain

import com.tencent.angel.ml.math2.vector.IntFloatVector
import Louvain.edgeTripleRDD2GraphPartitions
import com.tencent.angel.spark.util.VectorUtils
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel

import scala.collection.mutable.ArrayBuffer


/**
  * An implement of louvain algorithm(known as fast unfolding)
  *
  */

object Louvain {

  def edgeTripleRDD2GraphPartitions(tripleRdd: RDD[(Int, Int, Float)],
                                    model: LouvainPSModel = null,
                                    numPartition: Option[Int] = None,
                                    storageLevel: StorageLevel = StorageLevel.MEMORY_ONLY)
  : RDD[LouvainGraphPartition] = {

    val partNum = numPartition.getOrElse(tripleRdd.getNumPartitions)
    tripleRdd.flatMap { case (src, dst, wgt) =>
      Iterator((src, (dst, wgt)), (dst, (src, wgt)))
    }.groupByKey(partNum).mapPartitions { iter =>
      if (iter.nonEmpty) {
        val keys = new ArrayBuffer[Int]()
        val neighbors = new ArrayBuffer[Array[Int]]()
        val weights = new ArrayBuffer[Array[Float]]()
        iter.foreach { case (key, group) =>
          keys += key
          val (e, w) = group.unzip
          neighbors += e.toArray
          weights += w.toArray
        }
        Iterator.single((keys.toArray, neighbors.toArray, weights.toArray))
      } else {
        Iterator.empty
      }
    }.map { case (keys, neighbors, weights) =>
      // calc nodeWeights
      val nodeWeights = if (null != model) {
        model.getCommInfo(keys).get(keys)
      } else {
        weights.map(_.sum)
      }
      new LouvainGraphPartition(keys, neighbors, weights, nodeWeights)
    }.persist(storageLevel)
  }
}


class Louvain(
               @transient val graph: RDD[LouvainGraphPartition],
               louvainPSModel: LouvainPSModel) extends Serializable {

  private lazy val totalWeights: Double = {
    val total = graph.map(_.partitionWeights).sum()
    println(s"total=$total")
    total
  }

  // set community id as the the minimum id of nodes in it
  def correctCommunityId(model: LouvainPSModel): Unit = {
    commId2NodeId(model).groupByKey.foreachPartition { iter =>
      if (iter.nonEmpty) {
        val oldCommIdsBuffer = new ArrayBuffer[Int]()
        val newCommIdsBuffer = new ArrayBuffer[Int]()
        val nodesBuffer = new ArrayBuffer[Int]()
        val nodeNewCommIdsBuffer = new ArrayBuffer[Int]()
        iter.foreach { case (commId, nodes) =>
          oldCommIdsBuffer += commId
          val newId = nodes.min
          newCommIdsBuffer += newId
          nodesBuffer ++= nodes
          nodeNewCommIdsBuffer ++= Array.tabulate(nodes.size)(_ => newId)
        }
        val oldCommIds = oldCommIdsBuffer.toArray
        val newCommIds = newCommIdsBuffer.toArray
        val oldInfo = model.getCommInfo(oldCommIds).get(oldCommIds)
        // update node2comm
        model.updateNode2community(nodesBuffer.toArray, nodeNewCommIdsBuffer.toArray)
        // update comm2info
        model.incrementCommWeight(oldCommIds, oldInfo.map(x => -x))
        model.incrementCommWeight(newCommIds, oldInfo)
      }
    }
  }

  def checkCommId(model: LouvainPSModel): Long = {
    // check ids
    commId2NodeId(model).groupByKey.filter { case (commId, nodes) =>
      commId != nodes.min
    }.count()
  }

  def commId2NodeId(model: LouvainPSModel): RDD[(Int, Int)] = {
    graph.flatMap(_.partComm2nodeParis(model))
  }

  def checkTotalSum(model: LouvainPSModel): Double = {
    // check totalSum
    distinctCommIds(model).mapPartitions { iter =>
      if (iter.nonEmpty) {
        val arr = iter.toArray
        val s = model.community2weightPSVector.pull(arr).asInstanceOf[IntFloatVector].sum()
        Iterator(s)
      } else {
        Iterator(0.0)
      }
    }.sum()
  }

  def distinctCommIds(model: LouvainPSModel): RDD[Int] = {
    graph.flatMap(_.partCommunityIds(model)).distinct()
  }

  def save(path: String): Unit = {
    graph.flatMap(_.node2community(louvainPSModel))
      .map { case (id, comm) => s"$id,$comm" }
      .saveAsTextFile(path + s"/${System.currentTimeMillis()}")
  }

  def updateNodeWeightsToPS(): this.type = {
    graph.foreach(_.updateNodeWeightsToPS(louvainPSModel))
    this
  }

  def modularityOptimize(maxIter: Int, batchSize: Int, eps: Double = 0.0): Unit = {
    var curTime = System.currentTimeMillis()
    val q2 = Q2()
    val q1 = Q1()
    var deltaQ = q1 - q2
    var prevQ = deltaQ
    println(s"Q1=$q1, Q2=$q2, Q = ${q1 - q2}, takes ${System.currentTimeMillis() - curTime}ms")
    var optIter = 0
    while (optIter < maxIter && deltaQ > eps) {
      curTime = System.currentTimeMillis()
      modularityOptimize(batchSize, shuffle = optIter != 0)
      println(s"opt, takes ${System.currentTimeMillis() - curTime}ms")

      curTime = System.currentTimeMillis()
      val q1 = Q1()
      val q2 = Q2()
      val q = q1 - q2
      println(s"Q1=$q1, Q2=$q2, Q = $q, takes ${System.currentTimeMillis() - curTime}ms")
      curTime = System.currentTimeMillis()
      deltaQ = q - deltaQ
      prevQ = q
      println(s"numCommunity = $getNumOfCommunity, takes ${System.currentTimeMillis() - curTime}ms")
      optIter += 1
    }
  }

  private def Q2(): Double = {
    VectorUtils.dot(louvainPSModel.community2weightPSVector,
      louvainPSModel.community2weightPSVector
    ) / math.pow(totalWeights, 2)
  }

  private def modularityOptimize(batchSize: Int, shuffle: Boolean): Unit = {
    graph.foreach(_.modularityOptimize(louvainPSModel, totalWeights, batchSize, shuffle))
  }

  private def Q1(): Double = {
    (VectorUtils.sum(louvainPSModel.community2weightPSVector) -
      this.graph.map(_.sumOfWeightsBetweenCommunity(louvainPSModel)).sum()
      ) / totalWeights
  }

  private def getNumOfCommunity: Long = {
    this.graph.flatMap(_.node2community(louvainPSModel)).values.distinct().count()
  }

  def folding(batchSize: Int, storageLevel: StorageLevel): Louvain = {
    val curTime = System.currentTimeMillis()
    val newEdges = this.graph.flatMap { part =>
      part.partFolding(louvainPSModel, batchSize)
    }.reduceByKey(_ + _).map { case ((src, dst), wgt) =>
      (src, dst, wgt / 2.0f)
    }
    val newGraph = edgeTripleRDD2GraphPartitions(newEdges, louvainPSModel)
    newGraph.foreachPartition(_ => Unit)
    this.graph.unpersist()
    println(s"folding, takes ${System.currentTimeMillis() - curTime}ms")
    new Louvain(newGraph, louvainPSModel)
  }

  private def hist: (Array[Double], Array[Long]) = {
    this.graph.flatMap(_.node2community(louvainPSModel)).values.histogram(25)
  }
}
