package com.tencent.angel.spark.ml.graph.louvain

import com.tencent.angel.ml.math2.vector.IntFloatVector

import scala.collection.mutable.ArrayBuffer
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import com.tencent.angel.spark.context.PSContext
import com.tencent.angel.spark.ml.graph.louvain.Louvain.edgeTripleRDD2GraphPartitions
import com.tencent.angel.spark.util.VectorUtils


/**
  * An implement of louvain algorithm(known as fast unfolding)
  *
  */

object Louvain {

  def edgeTripleRDD2GraphPartitions(tripleRdd: RDD[(Int, Int, Float)],
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
      new LouvainGraphPartition(keys, neighbors, weights)
    }.persist(storageLevel)
  }



  def main(args: Array[String]): Unit = {

    val mode = "local"
    val input = "/home/wenbinwei/Downloads/email-Eu-core.txt"
    val partitionNum = 4
    val storageLevel = StorageLevel.MEMORY_ONLY
    val numFold = 3

    start(mode)
    val edges = SparkContext.getOrCreate().textFile(input, partitionNum).flatMap { line =>
      val arr = line.split("[\\s+,]")
      val src = arr(0).toInt
      val dst = arr(1).toInt
      if (src < dst) {
        Iterator(((src, dst), 1.0f))
      } else if (dst < src) {
        Iterator(((dst, src), 1.0f))
      } else {
        Iterator.empty
      }
    }.reduceByKey(_ + _).map{ case ((src, dst), wgt) => (src, dst, wgt)}
    var graph = edgeTripleRDD2GraphPartitions(edges, storageLevel = storageLevel)
    val maxId = graph.map(_.max).fold(Int.MinValue)(math.max)
    val model = LouvainPSModel.apply(maxId + 1)
    var louvain = new Louvain(graph, model)
    louvain.init(false)

    louvain.modularityOptimize(4)

    // correctIds
    var totalSum = louvain.checkTotalSum(model)
    louvain.correctCommunityId(model)
    assert(louvain.check(model) == 0)
    assert(louvain.checkTotalSum(model) == totalSum)
    var foldIter = 0
    while (foldIter < numFold) {
      foldIter += 1
      louvain = louvain.folding(100, storageLevel)
      louvain.init()
      louvain.modularityOptimize(4)

      // correctIds
      totalSum = louvain.checkTotalSum(model)
      louvain.correctCommunityId(model)
      assert(louvain.check(model) == 0)
      assert(louvain.checkTotalSum(model) == totalSum)
    }

    // save
    louvain.save("louvain")
    stop()
  }

  def start(mode: String = "local"): Unit = {
    val conf = new SparkConf()
    conf.setMaster(mode)
    conf.setAppName("louvain")
    val sc = new SparkContext(conf)
    sc.setLogLevel("WARN")
    PSContext.getOrCreate(sc)
  }

  def stop(): Unit = {
    PSContext.stop()
    SparkContext.getOrCreate().stop()
  }
}

class Louvain(
    @transient val graph: RDD[LouvainGraphPartition],
    louvainPSModel: LouvainPSModel) extends Serializable {


  private def initializePS(louvainPSModel: LouvainPSModel, preserve: Boolean = true): Unit = {
    graph.foreach(_.initializePS(louvainPSModel, preserve = preserve))
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

  def check(model: LouvainPSModel):Long = {
    // check ids
    commId2NodeId(model).groupByKey.filter { case (commId, nodes) =>
      commId != nodes.min
    }.count()
  }

  def commId2NodeId(model: LouvainPSModel): RDD[(Int, Int)] = {
    graph.flatMap(_.community2nodes(model))
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
    graph.flatMap(_.communityIds(model)).distinct()
  }


  private lazy val totalWeights: Double = {
    val total = graph.map(_.partitionWeights).sum()
    println(s"total=$total")
    total
  }

  private def Q2(): Double = {
    VectorUtils.dot(louvainPSModel.community2weightPSVector,
      louvainPSModel.community2weightPSVector
    ) / math.pow(totalWeights, 2)
  }

  private def modularityOptimize(): Unit = {
    graph.foreach(_.modularityOptimize(louvainPSModel, totalWeights))
  }

  private def Q1(): Double = {
    (VectorUtils.sum(louvainPSModel.community2weightPSVector)  -
      graph.map(_.sumOfWeightsBetweenCommunity(louvainPSModel)).sum()
      ) / totalWeights
  }

  private def numOfCommunity: Long = {
    graph.flatMap(_.node2community(louvainPSModel)).values.distinct().count()
  }

  private def hist: (Array[Double], Array[Long]) = {
    graph.flatMap(_.node2community(louvainPSModel)).values.histogram(25)
  }

  private def save(path: String): Unit = {
    graph.flatMap(_.node2community(louvainPSModel))
      .map { case (id, comm) => s"$id,$comm" }
      .saveAsTextFile(path)
  }

  def init(preserve: Boolean = true): this.type = {
    initializePS(louvainPSModel, preserve)
    this
  }

  def modularityOptimize(maxIter: Int): Unit = {
    println(s"Q2 = ${Q2()}, Q1=${Q1()}")
    for (_ <- 0 until maxIter) {
      modularityOptimize()
      val q1 = Q1()
      val q2 = Q2()
      println(s"Q1=$q1, Q2=$q2, Q = ${q1 - q2}")
      println(s"numCommunity = $numOfCommunity")
    }
  }

  def folding(batchSize: Int, storageLevel: StorageLevel): Louvain = {
    val newEdges = graph.flatMap { part =>
      part.createNewEdges(louvainPSModel, batchSize)
    }.reduceByKey(_ + _).map { case ((src, dst), wgt) =>
      (src, dst, wgt / 2.0f)
    }.persist(storageLevel)

    val newGraph = edgeTripleRDD2GraphPartitions(newEdges)
    new Louvain(newGraph, louvainPSModel)
  }
}
