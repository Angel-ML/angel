package com.tencent.angel.graph.embedding.struc2vec.test

import com.tencent.angel.graph.data.neighbor.NeighborDataOps
import com.tencent.angel.graph.embedding.struc2vec.StructureSimilarity
import com.tencent.angel.graph.utils.{GraphIO, Stats}
import com.tencent.angel.graph.utils.params.{HasArrayBoundsPath, HasBalancePartitionPercent, HasBatchSize, HasDstNodeIdCol, HasEpochNum, HasIsWeighted, HasMaxIteration, HasNeedReplicaEdge, HasOutputCoreIdCol, HasOutputNodeIdCol, HasPSPartitionNum, HasPartitionNum, HasSrcNodeIdCol, HasStorageLevel, HasUseBalancePartition, HasUseEdgeBalancePartition, HasWalkLength, HasWeightCol}
import com.tencent.angel.spark.context.PSContext
import org.apache.spark.SparkContext
import org.apache.spark.ml.Transformer
import org.apache.spark.ml.param.ParamMap
import org.apache.spark.ml.util.Identifiable
import org.apache.spark.sql.{DataFrame, Dataset, Row}
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.storage.StorageLevel

import scala.collection.mutable.ArrayBuffer

class Struc2Vec(override val uid: String) extends Transformer
  with HasSrcNodeIdCol with HasDstNodeIdCol with HasOutputNodeIdCol with HasOutputCoreIdCol
  with HasStorageLevel with HasPartitionNum with HasPSPartitionNum with HasMaxIteration
  with HasBatchSize with HasArrayBoundsPath with HasIsWeighted with HasWeightCol with HasUseBalancePartition
  with HasNeedReplicaEdge with HasUseEdgeBalancePartition with HasWalkLength with HasEpochNum with HasBalancePartitionPercent {
  private var output: String = _

  def this() = this(Identifiable.randomUID("Struc2Vec"))

  def setOutputDir(in: String): Unit = {
    output = in
  }

  override def transform(dataset: Dataset[_]): DataFrame = {

    //create origin edges RDD and data preprocessing

    val rawEdges = NeighborDataOps.loadEdgesWithWeight(dataset, $(srcNodeIdCol), $(dstNodeIdCol), $(weightCol), $(isWeighted), $(needReplicaEdge), true, false, false)
    rawEdges.repartition($(partitionNum)).persist(StorageLevel.DISK_ONLY)
    val (minId, maxId, numEdges) = Stats.summarizeWithWeight(rawEdges)
    println(s"minId=$minId maxId=$maxId numEdges=$numEdges level=${$(storageLevel)}")

    val edges = rawEdges.map { case (src, dst, w) => (src, (dst, w)) }
    val rmWeight = rawEdges.map{case (src, dst, w) => (src, dst)}

    // create adjacency table
    val adjTable = rmWeight.groupByKey()

    // create adjacency matrix
    val len:Int = adjTable.count().toInt
    val adjArray = adjTable.collect()
    val adjMatrix: Array[Array[Int]] = Array.ofDim(len,len)

    // initialize adjMatrix
    for (i <- 0 until len; j<- 0 until len)
           if(i==j)
             adjMatrix(i)(j) = 0
           else
             adjMatrix(i)(j) = Int.MaxValue

    for(item <- adjArray){
      for(j<-item._2)
        adjMatrix(item._1.toInt)(j.toInt) = 1
    }

    val structureSimilarity = new StructureSimilarity(adjMatrix)

//    val diam = structureSimilarity.diam
//    println(s"diam = $diam")
//    val hop = structureSimilarity.hopCountResult
//    val degrees = structureSimilarity.degrees

    structureSimilarity.compute()

    // get the structure similarity network
    val simi = structureSimilarity.structSimi

//    println("Layer1: ")
//    for (i <- 0 to simi(0).length - 1; j <- 0 to simi(0)(0).length - 1) {
//      print((i, j) + " = " + simi(0)(i)(j) + " ")
//    }
//    println("Layer5: ")
//    for (i <- 0 to simi(4).length - 1; j <- 0 to simi(0)(0).length - 1) {
//      print((i, j) + " = " + simi(4)(i)(j) + " ")
//    }

//    println("Degrees: ")
//    for (i <- 0 to degrees.length - 1) {
//      print("node"+ i + " = " + degrees(i)+ " ")
//    }

//    val fn =adjTable.take(17)(16)._2
//    val temp = adjArray(0)._2
//    val cnt = adjTable.count()
//    val ele = adjMatrix(32)(33)
//    println(s"get = $ele   count = $cnt")

    dataset.sparkSession.createDataFrame(edges.map(x => Row(x)), transformSchema(dataset.schema))

  }


  override def transformSchema(schema: StructType): StructType = {
    StructType(Seq(StructField("path",StringType, nullable = false)))
  }


  override def copy(extra: ParamMap): Transformer = defaultCopy(extra)



}

object Struc2Vec {
  def calcAliasTable(partId: Int, iter: Iterator[(Long, Array[(Long, Float)])]): Iterator[(Long, Array[Long], Array[Float], Array[Int])] = {
    iter.map { case (src, neighbors) =>
      val (events, weights) = neighbors.unzip
      val weightsSum = weights.sum
      val len = weights.length
      val areaRatio = weights.map(_ / weightsSum * len)
      val (accept, alias) = createAliasTable(areaRatio)
      (src, events, accept, alias)
    }
  }

  def createAliasTable(areaRatio: Array[Float]): (Array[Float], Array[Int]) = {
    val len = areaRatio.length
    val small = ArrayBuffer[Int]()
    val large = ArrayBuffer[Int]()
    val accept = Array.fill(len)(0f)
    val alias = Array.fill(len)(0)

    for (idx <- areaRatio.indices) {
      if (areaRatio(idx) < 1.0) small.append(idx) else large.append(idx)
    }
    while (small.nonEmpty && large.nonEmpty) {
      val smallIndex = small.remove(small.size - 1)
      val largeIndex = large.remove(large.size - 1)
      accept(smallIndex) = areaRatio(smallIndex)
      alias(smallIndex) = largeIndex
      areaRatio(largeIndex) = areaRatio(largeIndex) - (1 - areaRatio(smallIndex))
      if (areaRatio(largeIndex) < 1.0) small.append(largeIndex) else large.append(largeIndex)
    }
    while (small.nonEmpty) {
      val smallIndex = small.remove(small.size - 1)
      accept(smallIndex) = 1
    }

    while (large.nonEmpty) {
      val largeIndex = large.remove(large.size - 1)
      accept(largeIndex) = 1
    }
    (accept, alias)
  }

}