package com.tencent.angel.spark.ml.graph.algo.statistics.clickrate.onehop

import com.tencent.angel.spark.ml.graph.utils.params._
import org.apache.spark.ml.Transformer
import org.apache.spark.ml.param.ParamMap
import org.apache.spark.ml.util.Identifiable
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.{FloatType, LongType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Dataset}
import org.apache.spark.storage.StorageLevel

class OneHop(override val uid: String) extends Transformer
  with HasSrcNodeIdCol with HasDstNodeIdCol with HasBatchSize
  with HasPullBatchSize with HasUseBalancePartition with HasPSPartitionNum
  with HasBalancePartitionPercent with HasInput with HasExtraInputs
  with HasStorageLevel with HasPartitionNum with HasDelimiter
  with HasSrcNodeIndex with HasDstNodeIndex {
  
  def this() = this(Identifiable.randomUID("onehop"))
  
  override def transform(dataset: Dataset[_]): DataFrame = {
    val sc = dataset.sparkSession.sparkContext
    assert(sc.getCheckpointDir.nonEmpty, "set checkpoint dir first")

    println(s"=== 1. load user-user edges ===")
    val userEdges = OneHopOperator.loadUserEdges(dataset, $(srcNodeIdCol), $(dstNodeIdCol), true)
    
    userEdges.persist(StorageLevel.MEMORY_ONLY_SER)
    println(s"=== sample edges: ===")
    println(s"${userEdges.take(10).mkString(",")}")
    
    println(s"=== 2. convert edges to neighbor table ===")
    val userNeighborTables: RDD[(Long, Array[Long])] = OneHopOperator.edges2NeighborTable(userEdges, $(partitionNum), true)
      .persist($(storageLevel))
    
    println(s"sample neighbor tables:")
    userNeighborTables.take(10).foreach{ table =>
      println(s"src: ${table._1}, neighbors: ${table._2.mkString(",")}")
    }
    
    println(s"=== 3. calculate graph stats ===")
    val stats = OneHopOperator.calculateStats(userNeighborTables)
    println(s"min node id = ${stats._1}")
    println(s"max node id = ${stats._2}")
    println(s"num of nodes = ${stats._3}")
    println(s"num of edges = ${stats._4}")
    
    println(s"=== 4. start parameter server ===")
    val startPSTime = System.currentTimeMillis()
    OneHopOperator.startPS(sc)
    println(s"start parameter server costs ${System.currentTimeMillis() - startPSTime} ms")
    
    println(s"=== 5. push neighbor table to parameter server ===")
    val initPSTime = System.currentTimeMillis()
    val model = OneHopPSModel(stats._2 + 1, $(batchSize), $(pullBatchSize), $(psPartitionNum), stats._1)
    model.initLongNeighborTable(userNeighborTables)
    println(s"initializing neighbor table ps costs: ${System.currentTimeMillis() - initPSTime} ms")
    
    userNeighborTables.unpersist()
    
    val cpTableStartTime = System.currentTimeMillis()
    model.checkpoint()
    println(s"checkpoint of neighbor table costs ${System.currentTimeMillis() - cpTableStartTime} ms")
    
    println("test parameter server")
    val checkValid = OneHopOperator.testPS(userNeighborTables, model, 3)
    require(checkValid, s"result with executor RDD and that with PS are different")
  
    println(s"=== 6. load ad-user edges ===")
    val uAloadTime = System.currentTimeMillis()
    // extra input is user-ad pairs,
    // read and reform them to ad-user pairs
    val extraInput = $(extraInputs)
    assert(extraInput.length == 1, s"multiple inputs for non-friends are not supported")
  
    val adUserEdges = OneHopOperator.loadUserAdEdges(extraInput(0), $(srcNodeIndex),
      $(dstNodeIndex), $(srcNodeIdCol), $(dstNodeIdCol), $(delimiter))
    println(s"load user-ad edges costs: ${System.currentTimeMillis() - uAloadTime} ms")
    
    val adUserList = OneHopOperator.makeAdUserList(adUserEdges, $(partitionNum))
    
    println(s"=== 7. process ad-user partitions ===")
    val processStartTime = System.currentTimeMillis()
    val rawResults = adUserList.mapPartitionsWithIndex { (partId, iter) =>
      OneHopOperator.runAdPartitions(iter, partId, model)
    }
    println(s"process partitions costs: ${System.currentTimeMillis() - processStartTime} ms")
    
    println(s"sample results: ")
    rawResults.take(10).foreach{ res =>
      println(s"AD: ${res.getLong(0)}, OneHop: ${res.getFloat(1)}")
    }
    
    val outPutSchema = transformSchema(dataset.schema)
    dataset.sparkSession.createDataFrame(rawResults, outPutSchema)
  }
  
  override def transformSchema(schema: StructType): StructType = {
    StructType(
      Seq(
        StructField("ad column", LongType, nullable = false),
        StructField("one-hop click rate", FloatType, nullable = false)
      )
    )
  }
  
  override def copy(extra: ParamMap): Transformer = defaultCopy(extra)
  
}
