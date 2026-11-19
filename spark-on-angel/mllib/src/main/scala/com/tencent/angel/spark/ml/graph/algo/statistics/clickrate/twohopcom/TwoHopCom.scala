package com.tencent.angel.spark.ml.graph.algo.statistics.clickrate.twohopcom

import com.tencent.angel.ml.math2.VFactory
import com.tencent.angel.spark.context.PSContext
import com.tencent.angel.spark.ml.graph.utils.BatchIter
import com.tencent.angel.spark.ml.graph.utils.params._
import it.unimi.dsi.fastutil.longs.Long2LongOpenHashMap
import org.apache.spark.ml.Transformer
import org.apache.spark.ml.param.ParamMap
import org.apache.spark.ml.util.Identifiable
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.{FloatType, LongType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Dataset}
import org.apache.spark.storage.StorageLevel

import scala.collection.mutable

class TwoHopCom(override val uid: String) extends Transformer
  with HasSrcNodeIdCol with HasDstNodeIdCol with HasSrcNodeIndex
  with HasDstNodeIndex with HasBatchSize with HasPullBatchSize
  with HasBalancePartitionPercent with HasDelimiter with HasStorageLevel
  with HasPartitionNum with HasPSPartitionNum with HasExtraInputs {
  def this() = this(Identifiable.randomUID("twohopcomclickrate"))
  
  override def transform(dataset: Dataset[_]): DataFrame = {
    val sc = dataset.sparkSession.sparkContext
    
    val uuedges = dataset.select($(srcNodeIdCol), $(dstNodeIdCol)).rdd
      .filter(row => !row.anyNull)
      .map(row => (row.getLong(0), row.getLong(1)))
      .filter(e => e._1 != e._2)
    uuedges.persist(StorageLevel.MEMORY_ONLY_SER)
    
    val nbrTables = uuedges.flatMap(f => Iterator((f._1, f._2), (f._2, f._1)))
      .groupByKey($(partitionNum))
      .map(table => (table._1, table._2.toArray.distinct.sorted))
    
    nbrTables.persist($(storageLevel))
    uuedges.unpersist(blocking = false)
    
    // extra inputs user-community and user-ad
    val extraInput = $(extraInputs)
    assert(extraInput.length == 2, s"two extra input path needed")
    
    val userCom = TwoHopComOperator.loadUserComEdges(extraInput(0), $(srcNodeIndex),
      $(dstNodeIndex), $(srcNodeIdCol), $(dstNodeIdCol), $(delimiter))
    userCom.persist($(storageLevel))
    
    // calculate stats
    val stats = TwoHopComOperator.statsFromUserCom(userCom)
    println(s"minId=${stats._1},maxId=${stats._2},numNodes=${stats._3}")
    
    // start ps
    PSContext.getOrCreate(sc)
    val model = TwoHopComPSModel(stats._2 + 1, $(batchSize), $(pullBatchSize), $(psPartitionNum),
      userCom.map(_._1))
    
    // init user-community on ps
    userCom.foreachPartition{ iter =>
      val uCom = VFactory.sparseLongKeyLongVector(model.dim)
      iter.foreach{ pair =>
        uCom.set(pair._1, pair._2)
      }
      model.initUserCom(uCom)
    }
    
    // make community guided neighbor table
    val comNbrTable = nbrTables.mapPartitions{ iter =>
      BatchIter(iter, $(batchSize)).flatMap{ batchIter =>
        val pullNodes = mutable.HashSet[Long]()
        batchIter.foreach{ tb =>
          pullNodes += tb._1
          pullNodes ++= tb._2
        }
        
        val uComMsgs = model.getUserCom(pullNodes.toArray)
        batchIter.flatMap{ tb =>
          val srcLabel = uComMsgs.get(tb._1)
          val newTable = tb._2.filter(uComMsgs.get(_) == srcLabel)
          Iterator.single((tb._1, newTable))
        }
      }
    }
    
    // init com-nbr table on ps
    model.initLongNeighborTable(comNbrTable)
    
    model.checkpoint()
    
    nbrTables.unpersist(blocking = false)
    userCom.unpersist(blocking = false)
  
    val adUsers = TwoHopComOperator.loadAdUserEdges(extraInput(1), $(srcNodeIndex),
      $(dstNodeIndex), $(srcNodeIdCol), $(dstNodeIdCol), $(delimiter))
    adUsers.persist($(storageLevel))
  
    val adUsersList = adUsers.groupByKey($(partitionNum))
      .map(adu => (adu._1, adu._2.toArray.distinct.sorted))
  
    val rawResults = adUsersList.mapPartitionsWithIndex { (partId, iter) =>
      TwoHopComOperator.runAdPartitions(iter, partId, model)
    }
  
    val outPutSchema = transformSchema(dataset.schema)
    dataset.sparkSession.createDataFrame(rawResults, outPutSchema)
    
  }
  
  override def transformSchema(schema: StructType): StructType = {
    StructType(
      Seq(
        StructField("node index", LongType, nullable = false),
        StructField("two-hop rate", FloatType, nullable = false)
      )
    )
  }
  
  override def copy(extra: ParamMap): Transformer = defaultCopy(extra)
}
