package com.tencent.angel.spark.ml.graph.algo.cliques.kclique

import com.tencent.angel.spark.ml.graph.GraphOps
import com.tencent.angel.spark.ml.graph.utils.params._
import org.apache.spark.ml.Transformer
import org.apache.spark.ml.param.{IntParam, ParamMap}
import org.apache.spark.ml.util.Identifiable
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Dataset, Row}

import scala.collection.mutable.ArrayBuffer

class KClique(override val uid: String) extends Transformer
  with HasSrcNodeIdCol with HasDstNodeIdCol with HasPartitionNum with HasPSPartitionNum
  with HasStorageLevel with HasBatchSize with HasPullBatchSize
  with HasBufferSize with HasCommonFriendsNumCol with HasDebugMode
  with HasSrcNodeIndex with HasDstNodeIndex
  with HasInput  {
  
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
      .flatMap{ case (srcId, dstId) => Iterator((srcId, dstId), (dstId, srcId)) }.distinct()
    
    println(s"sample edges = ")
    println(edges.take(10).mkString(","))
    
    println(s"2. convert edges to neighbor table")
    val neighborRDD = GraphOps.edgesToNeighborTable(edges, $(partitionNum))
    
    println(s"3. build k-clique partition")
    val kCliquePartition = KCliqueGraphPartition.fromNeighborTableRDD(neighborRDD)
    kCliquePartition.persist($(storageLevel))
    
    println(s"4. calculate stats of graph")
    val stats = KCliqueGraphPartition.getStats(kCliquePartition)
    println(stats)
  
    println(s"5：start parameter server")
    val psStartTime = System.currentTimeMillis()
    KCliquePSModel.startPS(dataset.sparkSession.sparkContext)
    println(s"start parameter server costs ${System.currentTimeMillis() - psStartTime} ms")
  
    println(s"6：push 2-clique info to parameter server")
    val initTableStartTime = System.currentTimeMillis()
    val kcliquePSModel = KCliquePSModel(stats.maxVertexId + 1, $(batchSize), $(pullBatchSize), $(psPartitionNum))
    kcliquePSModel.init2Clique(kCliquePartition)
    kcliquePSModel.testPS(kCliquePartition, 2)
    println(s"push costs ${System.currentTimeMillis() - initTableStartTime} ms")
  
    println(s"7. calculate k-clique")
    val algoStartTime = System.currentTimeMillis()
    var k = 2
    var numCliques = 0L
    do {
      k += 1
      numCliques = kCliquePartition.map(_.calculateKClique(kcliquePSModel, k)).reduce((n1, n2) => n1 + n2)

      kcliquePSModel.resetMsgs()
      //kCliquePartition.map(_.resetCliques(kcliquePSModel))
      var cntTmp1 = kCliquePartition.map(_.countNodes(kcliquePSModel)).reduce((n1, n2) => n1 + n2)
      println(s"!!!!cnt1 nodes with msgs: $cntTmp1")
      
      println(s"num $k-Cliques: $numCliques")
    } while (k < $(maxK) && numCliques > 0)
  
    println(s"$k-clique generated, ${$(maxK)} required but no kclique more than $k")
    println(s"process k-clique costs ${System.currentTimeMillis() - initTableStartTime} ms")
  
    println(s"8. save k-clique results")
//    val retRDD = kCliquePartition.map(_.save2(kcliquePSModel, k)).flatMap(f => f._1.zip(f._2))
//      .map(f => Row.fromSeq(Seq[Any](f._1, f._2)))

    val retRDD = kCliquePartition.flatMap(_.save(kcliquePSModel, k)).map(f => Row.fromSeq(f.toSeq))
    val outputSchema = makeSchema(k)
//    val outputSchema = transformSchema(dataset.schema)
    dataset.sparkSession.createDataFrame(retRDD, outputSchema)
    
  }
  
  override def transformSchema(schema: StructType): StructType = {
    StructType(Seq(
      StructField("cliqueId", LongType, nullable = false),
      StructField("nodeId", LongType, nullable = false)
//
       //StructField("nodeId", ObjectType(classOf[Array[Long]]), nullable = false)
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

