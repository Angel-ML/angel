package com.tencent.angel.spark.ml.graph.algo.cliques.kclique0

import com.tencent.angel.spark.ml.graph.utils.params._
import org.apache.spark.ml.Transformer
import org.apache.spark.ml.param.{IntParam, ParamMap}
import org.apache.spark.ml.util.Identifiable
import org.apache.spark.sql.types.{LongType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Dataset, Row}
import org.apache.spark.storage.StorageLevel

import scala.collection.mutable.ArrayBuffer

class KClique(override val uid: String) extends Transformer
  with HasSrcNodeIdCol with HasDstNodeIdCol with HasPullBatchSize
  with HasUseBalancePartition with HasPartitionNum
  with HasPSPartitionNum with HasStorageLevel
  with HasBatchSize with HasBalancePartitionPercent {

  final val maxK = new IntParam(this, "maxK", "maxK")
  final def setMaxK(num: Int): this.type = set(maxK, num)
  setDefault(maxK, 4)

  def this() = this(Identifiable.randomUID("k-clique"))

  override def transform(dataset: Dataset[_]): DataFrame = {

    val edges = dataset.select($(srcNodeIdCol), $(dstNodeIdCol)).rdd
      .filter(row => !row.anyNull)
      .map(row => (row.getLong(0), row.getLong(1)))
      .filter(e => e._1 != e._2)
    edges.persist(StorageLevel.MEMORY_ONLY_SER)


    println(s" calculate graph stats")
    val statsCalStartTime = System.currentTimeMillis()
    val nodes = edges.flatMap(e => Iterator(e._1, e._2))
    val maxId = nodes.max() + 1
    val minId = nodes.min()
    val numEdges = edges.count()
    println(s"maxId=$maxId, minId=$minId, numEdges=$numEdges, level=${$(storageLevel)}")
    println(s"calculate stats costs ${System.currentTimeMillis() - statsCalStartTime} ms")

    val psStartTime = System.currentTimeMillis()
    KCliquePSModel.startPS(dataset.sparkSession.sparkContext)
    println(s"start parameter server costs ${System.currentTimeMillis() - psStartTime} ms")

    println(s"create ps model")
    val psCreateTime = System.currentTimeMillis()
    val kcqModel = KCliquePSModel.fromMinMax(minId, maxId, nodes, $(psPartitionNum), $(useBalancePartition), $(balancePartitionPercent))
    println(s"create ps model cost ${System.currentTimeMillis() - psCreateTime} ms")

    println(s"build k-clique partition")
    val buildPartTime = System.currentTimeMillis()
    var graph = edges.flatMap { case (srcId, dstId) => Iterator((srcId, dstId), (dstId, srcId)) }
      .groupByKey($(partitionNum))
      .mapPartitionsWithIndex((index, adjTable) => Iterator((0L, KCliqueGraphPartition.apply(index, adjTable))))
    graph.persist($(storageLevel))

    println(s"build ${$(partitionNum)} partitions costs ${System.currentTimeMillis() - buildPartTime} ms")

    println(s"6. init ps with neighbors")
    val psInitTime = System.currentTimeMillis()
    graph.foreach(_._2.initPSNbrs(kcqModel, $(batchSize)))
    println(s"push neighbors to ps costs ${System.currentTimeMillis() - psInitTime} ms")
    assert($(maxK) > 2)

    kcqModel.testPS(graph.map(_._2), 2)

    println(s"7. process k-cliques")
    val processStartTime = System.currentTimeMillis()
    var nK = 2
    var numCliques = 0L
    var prevGraph = graph
    do {
      nK += 1
      println(s"start process $nK-cliques")
      graph = prevGraph.map(_._2.process(kcqModel, nK, $(pullBatchSize)))
      graph.persist($(storageLevel))
      numCliques = graph.map(_._1).sum.toLong
      prevGraph.unpersist()
      prevGraph = graph
      println(s"num $nK-cliques: $numCliques")

    } while(nK < $(maxK) && numCliques > 0)

    println(s"$nK-clique generated, ${$(maxK)} required but no kclique more than $nK")
    println(s"process k-clique costs ${System.currentTimeMillis() - processStartTime} ms")

    println(s"8. save k-clique results")
    val retRDD = graph.map(_._2.save()).flatMap(f => f._1.zip(f._2)).filter(_._2.length > 0)
      .map(f => Row.fromSeq(f._1 +: f._2.toSeq))
    val outputSchema = makeSchema(nK)

    dataset.sparkSession.createDataFrame(retRDD, outputSchema)

  }


  def makeSchema(k: Int): StructType = {
    var sf = ArrayBuffer[StructField]()
    for (i <- 1 to k) {
      sf += StructField(i.toString, LongType, nullable = false)
    }
    StructType(sf)
  }

  override def transformSchema(schema: StructType): StructType ={
    StructType{
      Seq()
    }
  }

  override def copy(extra: ParamMap): Transformer = defaultCopy(extra)
}
