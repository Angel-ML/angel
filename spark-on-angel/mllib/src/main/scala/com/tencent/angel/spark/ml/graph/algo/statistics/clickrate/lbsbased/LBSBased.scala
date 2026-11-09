package com.tencent.angel.spark.ml.graph.algo.statistics.clickrate.lbsbased

import com.tencent.angel.spark.context.PSContext
import com.tencent.angel.spark.ml.graph.utils.params._
import org.apache.spark.ml.Transformer
import org.apache.spark.ml.param.ParamMap
import org.apache.spark.ml.util.Identifiable
import org.apache.spark.sql.types.{FloatType, LongType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Dataset}
import org.apache.spark.storage.StorageLevel

class LBSBased(override val uid: String) extends Transformer
  with HasInput with HasBatchSize with HasPullBatchSize
  with HasExtraInputs with HasPartitionNum with HasPSPartitionNum
  with HasSrcNodeIndex with HasSrcNodeIdCol with HasDstNodeIndex
  with HasDstNodeIdCol with HasUseBalancePartition with HasBalancePartitionPercent
  with HasDelimiter with HasStorageLevel {
  
  def this() = this(Identifiable.randomUID("LBSBasedClickrate"))
  
  override def transform(dataset: Dataset[_]): DataFrame = {
    val sc = dataset.sparkSession.sparkContext
    assert(sc.getCheckpointDir.nonEmpty, "set checkpoint dir first")
  
    println("=== 1. load user-ad edges ===")
    val userAdEdges = dataset.select($(srcNodeIdCol), $(dstNodeIdCol)).rdd
      .filter(row => !row.anyNull)
      .map(row => (row.getLong(0), row.getLong(1)))
    
    userAdEdges.persist(StorageLevel.MEMORY_ONLY_SER)
    println(s"sample edges:")
    println(s"${userAdEdges.take(10).mkString(",")}")
    
    println("=== 2. convert edges to user ads table ===")
    val userAdsTable = userAdEdges.groupByKey($(partitionNum)).map{ iter =>
      (iter._1, iter._2.toArray.distinct)
    }.persist($(storageLevel))
    
    userAdEdges.unpersist(blocking = false)
    
    println("=== 3. calculate graph stats ===")

    val (minId, maxId) = userAdsTable.map{ table =>
      (table._1, table._1)
    }.reduce((m1, m2) => (math.min(m1._1, m2._1), math.max(m1._2, m2._2)))
    println(s"minId: $minId, maxId: $maxId")
    
    println("=== 4. start parameter server ===")
    val startPSTime = System.currentTimeMillis()
    PSContext.getOrCreate(sc)
    println(s"start parameter server costs ${System.currentTimeMillis() - startPSTime} ms")
    
    println("=== 5. push user-ads to parameter server ===")
    val initPSTime = System.currentTimeMillis()
    val model = LBSBasedPSModel(maxId + 1, $(batchSize), $(pullBatchSize), $(psPartitionNum), minId)
    model.initLongAdTables(userAdsTable)
    println(s"initializing user ads table on parameter server costs: ${System.currentTimeMillis() - initPSTime} ms")
  
    userAdsTable.unpersist(blocking = false)
  
    val cpTableStartTime = System.currentTimeMillis()
    model.checkpoint()
    println(s"checkpoint of neighbor table costs ${System.currentTimeMillis() - cpTableStartTime} ms")
  
    //todo test ps
    
    println("=== 6. load LBS-user edges ===")
    val extraInput = $(extraInputs)
    assert(extraInput.length == 1, s"multiple inputs for non-friends are not supported")
  
    val LBSUserEdges = LBSBasedOperator.loadLBSUserEdges(extraInput(0),
      $(srcNodeIndex), $(dstNodeIndex), $(srcNodeIdCol), $(dstNodeIdCol), $(delimiter))
    val LBSUserTable = LBSUserEdges.groupByKey($(partitionNum)).map{ iter =>
      (iter._1, iter._2.toArray.distinct)
    }.persist($(storageLevel))
    
    println("=== 7. process LBS-Users Partitions ===")
    val processStartTime = System.currentTimeMillis()
    val rawResults = LBSUserTable.mapPartitionsWithIndex { (partId, iter) =>
      LBSBasedOperator.runLBSPartitions(iter, partId, model)
    }
    println(s"process partitions costs: ${System.currentTimeMillis() - processStartTime} ms")
  
    println("=== 8. save results ===")
    val outSchema = transformSchema(dataset.schema)
    dataset.sparkSession.createDataFrame(rawResults, outSchema)
  
  }
  
  override def transformSchema(schema: StructType): StructType = {
    StructType(
      Seq(
        StructField("LBS name", LongType, nullable = false),
        StructField("AD id", LongType, nullable = false),
        StructField("click rate", FloatType, nullable = false)
      )
    )
  }
  
  override def copy(extra: ParamMap): Transformer = defaultCopy(extra)
  
}
