package com.tencent.angel.spark.ml.graph.louvain

import com.tencent.angel.ml.math2.vector.IntFloatVector
import com.tencent.angel.spark.ml.graph.louvain.LouvainGraph.edgeTripleRDD2GraphPartitions
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel

import scala.collection.mutable.ArrayBuffer


/**
  * An implement of louvain algorithm(known as fast unfolding)
  *
  */

object LouvainGraph {

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


class LouvainGraph(
                    @transient val graph: RDD[LouvainGraphPartition],
                    louvainPSModel: LouvainPSModel) extends Serializable {

  private lazy val totalWeights: Double = {
    val total = graph.map(_.partitionWeights).sum()
    println(s"totalNodeWeights=$total")
    total
  }

  // set community id as the the minimum id of nodes in it
  def correctCommunityId(model: LouvainPSModel, bufferSize: Int = 1000000): Unit = {
    val commInfoDeltaRDD = commId2NodeId(model).groupByKey.mapPartitions { iter =>
      if (iter.nonEmpty) {
        val oldCommIdsBuffer = new ArrayBuffer[Int]()
        val newCommIdsBuffer = new ArrayBuffer[Int]()

        val nodesBuffer = new ArrayBuffer[Int](bufferSize)
        val nodeNewCommIdsBuffer = new ArrayBuffer[Int](bufferSize)
        var numNodeCorrected = 0
        while (iter.hasNext) {
          var used = 0
          while (iter.hasNext && used < bufferSize) {
            val (commId, nodes) = iter.next
            val newId = nodes.min

            oldCommIdsBuffer += commId
            newCommIdsBuffer += newId

            nodesBuffer ++= nodes
            nodeNewCommIdsBuffer ++= Array.tabulate(nodes.size)(_ => newId)

            used += nodes.size
            numNodeCorrected += 1
          }
          if (used > 0) {
            // update node2comm
            println(s"numNodeCorrected=$numNodeCorrected")
            model.updateNode2community(nodesBuffer.toArray, nodeNewCommIdsBuffer.toArray)
            nodesBuffer.clear()
            nodeNewCommIdsBuffer.clear()
          }
        }

        val oldCommIds = oldCommIdsBuffer.toArray
        val oldInfo = model.getCommInfo(oldCommIds).get(oldCommIds)
        Iterator((oldCommIds, oldInfo.map(x => -x)), (newCommIdsBuffer.toArray, oldInfo))
      } else {
        Iterator.empty
      }
    }.cache()

    // since we calculate delta from old community info from ps,
    // the old community info should preserve before all task has finished.
    // here we use a handy trigger job for this purpose.
    commInfoDeltaRDD.foreachPartition(_ => Unit)

    // update community info
    commInfoDeltaRDD.foreach { case (commIds, commInfoDelta) =>
      model.incrementCommWeight(commIds, commInfoDelta)
    }
    commInfoDeltaRDD.unpersist(false)
  }

  def commId2NodeId(model: LouvainPSModel): RDD[(Int, Int)] = {
    graph.flatMap(_.partComm2nodeParis(model))
  }

  def checkCommId(model: LouvainPSModel): Long = {
    // check ids
    commId2NodeId(model).groupByKey.filter { case (commId, nodes) =>
      commId != nodes.min
    }.count()
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

  def save(path: String, nodesRDD: RDD[Int]): Unit = {
    nodesRDD.mapPartitions { iter =>
      val nodes = iter.toArray
      val pair = nodes.zip(louvainPSModel.getNode2commMap(nodes).get(nodes))
      pair.map { case (id, comm) => s"$id,$comm" }.toIterator
    }.saveAsTextFile(path)
  }

  def updateNodeWeightsToPS(): this.type = {
    graph.foreach(_.updateNodeWeightsToPS(louvainPSModel))
    this
  }

  def modularityOptimize(maxIter: Int, batchSize: Int, eps: Double = 0.0): Unit = {
    var curTime = System.currentTimeMillis()
    val q2 = Q2()
    val q1 = Q1()
    var deltaQ = Double.MaxValue
    var prevQ = q1 - q2
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
      deltaQ = q - prevQ
      prevQ = q
      println(s"numCommunity = $getNumOfCommunity, takes ${System.currentTimeMillis() - curTime}ms")
      optIter += 1
    }
  }

  private def Q2(): Double = {
    louvainPSModel.sumOfSquareOfCommunityWeights / math.pow(totalWeights, 2)
  }

  private def modularityOptimize(batchSize: Int, shuffle: Boolean): Unit = {
    graph.foreach(_.modularityOptimize(louvainPSModel, totalWeights, batchSize, shuffle))
  }

  private def Q1(): Double = {
    1 - sumOfWeightsBetweenCommunity / totalWeights
  }

  private def sumOfWeightsBetweenCommunity: Double = {
    this.graph.map(_.sumOfWeightsBetweenCommunity(louvainPSModel)).sum()
  }

  private def getNumOfCommunity: Long = {
    this.graph.flatMap(_.node2community(louvainPSModel)).values.distinct().count()
  }

  def folding(batchSize: Int, storageLevel: StorageLevel): LouvainGraph = {
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
    new LouvainGraph(newGraph, louvainPSModel)
  }

  private def hist: (Array[Double], Array[Long]) = {
    this.graph.flatMap(_.node2community(louvainPSModel)).values.histogram(25)
  }
}
