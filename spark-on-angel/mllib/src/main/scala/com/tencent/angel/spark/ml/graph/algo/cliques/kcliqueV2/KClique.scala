package com.tencent.angel.spark.ml.graph.algo.cliques.kcliqueV2

import com.tencent.angel.spark.ml.graph.utils.params._
import org.apache.spark.ml.Transformer
import org.apache.spark.ml.param.{IntParam, ParamMap}
import org.apache.spark.ml.util.Identifiable
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Dataset, Row}
import org.apache.spark.storage.StorageLevel

import scala.collection.mutable.ArrayBuffer

class KClique(override val uid: String) extends Transformer
  with HasSrcNodeIdCol with HasDstNodeIdCol with HasPartitionNum with HasPSPartitionNum
  with HasStorageLevel with HasPullBatchSize
  with HasSrcNodeIndex with HasDstNodeIndex with HasUseBalancePartition
  with HasInput with HasBalancePartitionPercent {
  
  final val maxK = new IntParam(this, "maxK", "maxK")
  final def setMaxK(num: Int): this.type = set(maxK, num)
  setDefault(maxK, 4)
  
  def this() = this(Identifiable.randomUID("k-clique"))
  
  override def transform(dataset: Dataset[_]): DataFrame = {
    val sc = dataset.sparkSession.sparkContext
    assert(sc.getCheckpointDir.nonEmpty, "set checkpoint dir first")
    println(s"partition number: ${$(partitionNum)}")
    
    println("1. load edges from the dataset")
    val edges = dataset.select($(srcNodeIdCol), $(dstNodeIdCol)).rdd
      .filter(row => !row.anyNull)
      .map(row => (row.getLong(0), row.getLong(1)))
      .filter(e => e._1 != e._2)

    // need to set spark.rdd.compress = true
    edges.persist(StorageLevel.MEMORY_ONLY_SER)
    
    println(s"sample edges = ")
    println(edges.take(10).mkString(","))
    
    println(s"2. calculate graph stats")
    val maxId = edges.map(e => math.max(e._1, e._2)).max() + 1
    val minId = edges.map(e => math.min(e._1, e._2)).min()
    val nodes = edges.flatMap(e => Iterator(e._1, e._2))
    val numEdges = edges.count()
    println(s"maxId=$maxId, minId=$minId, numEdges=$numEdges, level=${$(storageLevel)}")
  
    println(s"3：start parameter server")
    val psStartTime = System.currentTimeMillis()
    KCliquePSModel.startPS(dataset.sparkSession.sparkContext)
    println(s"start parameter server costs ${System.currentTimeMillis() - psStartTime} ms")
  
    println(s"4. create ps model")
    val psCreateTime = System.currentTimeMillis()
    val kcqModel = KCliquePSModel.fromMinMax(minId, maxId, nodes, $(psPartitionNum), $(useBalancePartition), $(balancePartitionPercent))
    println(s"create ps model cost ${System.currentTimeMillis() - psCreateTime} ms")
    
    println(s"5. build k-clique partition")
    var graph = edges.flatMap { case (srcId, dstId) => Iterator((srcId, dstId), (dstId, srcId)) }
      .groupByKey($(partitionNum))
      .mapPartitionsWithIndex((index, adjTable) => Iterator((0L, KCliqueGraphPartition.apply(index, adjTable, $(pullBatchSize)))))
    graph.persist($(storageLevel))
    
    println(s"6. init ps msgs with 2-cliques")
    val psInitTime = System.currentTimeMillis()
    graph.foreach(_._2.initMsgs(kcqModel))
    println(s"init ps msgs cost ${System.currentTimeMillis() - psInitTime} ms")
    
    kcqModel.testPS(graph.map(_._2), 2)
    
    println(s"7. calculate k-clique")
    val algoStartTime = System.currentTimeMillis()
    var k = 2
    var numCliques = 0L
    var prevGraph = graph
    do {
      k += 1
      println(s"start process $k-clique")
      val startTs = System.currentTimeMillis()
      graph = prevGraph.map(_._2.calculateKClique(kcqModel, k))
      graph.persist($(storageLevel))
      numCliques = graph.map(_._1).sum.toLong
      prevGraph.unpersist()
      kcqModel.resetMsgs()
      prevGraph = graph
      
      println(s"process $k-cliques costs: ${System.currentTimeMillis() - startTs} ms")
      println(s"num $k-Cliques: $numCliques")
    } while (k < $(maxK) && numCliques > 0)
  
    println(s"$k-clique generated, ${$(maxK)} required but no kclique more than $k")
    println(s"process k-clique costs ${System.currentTimeMillis() - algoStartTime} ms")
  
    println(s"8. save k-clique results")
    val retRDD = graph.flatMap(_._2.save(kcqModel, k)).map(f => Row.fromSeq(f.toSeq))
    val outputSchema = makeSchema(k)

    dataset.sparkSession.createDataFrame(retRDD, outputSchema)
    
  }
  
  override def transformSchema(schema: StructType): StructType = {
    StructType(Seq(
      StructField("cliqueId", LongType, nullable = false),
      StructField("nodeId", LongType, nullable = false)
    ))
  }
  
  def makeSchema(k: Int): StructType = {
    var sf = ArrayBuffer[StructField]()
    for (i <- 1 to k) {
      sf += StructField(i.toString, LongType, nullable = false)
    }
    StructType(sf)
  }
  
  override def copy(extra: ParamMap): Transformer = defaultCopy(extra)
}

