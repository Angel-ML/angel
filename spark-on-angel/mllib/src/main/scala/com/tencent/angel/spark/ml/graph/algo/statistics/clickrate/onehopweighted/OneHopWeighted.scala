package com.tencent.angel.spark.ml.graph.algo.statistics.clickrate.onehopweighted

import com.tencent.angel.spark.context.PSContext
import com.tencent.angel.spark.ml.graph.utils.BatchIter
import com.tencent.angel.spark.ml.graph.utils.params._
import org.apache.spark.ml.Transformer
import org.apache.spark.ml.param.ParamMap
import org.apache.spark.ml.util.Identifiable
import org.apache.spark.sql.types.{FloatType, LongType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Dataset}
import org.apache.spark.storage.StorageLevel

class OneHopWeighted(override val uid: String) extends Transformer
  with HasSrcNodeIndex with HasSrcNodeIdCol with HasDstNodeIndex
  with HasDstNodeIdCol with HasWeightCol with HasPartitionNum
  with HasPSPartitionNum with HasBalancePartitionPercent with HasUseBalancePartition
  with HasDelimiter with HasExtraInputs with HasBatchSize with HasPullBatchSize
  with HasStorageLevel {
  def this() = this(Identifiable.randomUID("onehopweighted"))
  
  override def transform(dataset: Dataset[_]): DataFrame = {
    val sc = dataset.sparkSession.sparkContext
    assert(sc.getCheckpointDir.nonEmpty, "set checkpoint dir first")
  
    // src:Long, dst:Long, weight:Float
    println("===1. load user edges ===")
    val userEdges = dataset.select($(srcNodeIdCol), $(dstNodeIdCol), $(weightCol)).rdd
        .filter(row => !row.anyNull)
        .map(row => (row.getLong(0), row.getLong(1), row.getFloat(2)))
        .filter(e => e._1 != e._2)
    
    userEdges.persist(StorageLevel.MEMORY_ONLY_SER)
    
    println("===2. convert user edges to graph ===")
    val usrGraphWeighted = userEdges.flatMap(f => Iterator((f._1, (f._2, f._3)), (f._2, (f._1, f._3))))
      .groupByKey($(partitionNum))
    usrGraphWeighted.persist($(storageLevel))
    
    println("===3. calculate graph stats ===")
    val maxId = userEdges.map(e => math.max(e._1, e._2)).max() + 1
    val minId = userEdges.map(e => math.min(e._1, e._2)).min()
    val nodes = userEdges.flatMap(e => Iterator(e._1, e._2))
    val numEdges = userEdges.count()
    println(s"minId:$minId, maxId:$maxId")
    userEdges.unpersist(blocking = false)
    
    println("===4. start parameter server ===")
    PSContext.getOrCreate(sc)
    
    println("===5. create ps model and init ps model ===")
    val model = OneHopWeightedPSModel.fromMinMax(minId, maxId, nodes, $(psPartitionNum), $(useBalancePartition))
    usrGraphWeighted.foreachPartition{ iter =>
      BatchIter(iter, $(batchSize)).foreach{ bIter =>
        OneHopWeightedOperator.initPSWithNbrAttr(model, bIter)
      }
    }
    usrGraphWeighted.unpersist(blocking = false)
  
    println("===6. load ad user map ===")
    val extraInput = $(extraInputs)
    assert(extraInput.length == 1, s"multiple inputs for non-friends are not supported")
  
    val adUserEdges = OneHopWeightedOperator.loadAdUserEdges(extraInput(0), $(srcNodeIndex),
      $(dstNodeIndex), $(srcNodeIdCol), $(dstNodeIdCol), $(delimiter))
  
    val adUserList = adUserEdges.groupByKey($(partitionNum))
      .map(adu => (adu._1, adu._2.toArray.distinct))
      .persist($(storageLevel))
    
    println("===7. process ad partitions")
    val rawResults = adUserList.mapPartitionsWithIndex { (partId, iter) =>
      OneHopWeightedOperator.runAdPartitions(iter, partId, model, $(pullBatchSize))
    }
    
    adUserList.unpersist(blocking = false)
  
    dataset.sparkSession.createDataFrame(rawResults, transformSchema(dataset.schema))
  }
  
  override def transformSchema(schema: StructType): StructType = {
    StructType(
      Seq(
        StructField("ad", LongType, nullable = false),
        StructField("click rate", FloatType, nullable = false)
      )
    )
  }
  
  override def copy(extra: ParamMap): Transformer = defaultCopy(extra)
  
}
