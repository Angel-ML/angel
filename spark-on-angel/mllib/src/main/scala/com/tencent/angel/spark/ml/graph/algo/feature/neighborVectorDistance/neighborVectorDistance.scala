package com.tencent.angel.spark.ml.graph.algo.feature.neighborVectorDistance

import com.tencent.angel.graph.common.param.ModelContext
import com.tencent.angel.graph.data.neighbor.NeighborDataOps
import com.tencent.angel.spark.context.PSContext
import com.tencent.angel.spark.ml.ann.params._
import com.tencent.angel.spark.ml.ann.utils.DataUtils
import com.tencent.angel.spark.ml.graph.utils.Stats
import com.tencent.angel.spark.ml.graph.utils.params.{HasBalancePartitionPercent, HasUseBalancePartition, _}
import org.apache.spark.SparkContext
import org.apache.spark.ml.Transformer
import org.apache.spark.ml.param.ParamMap
import org.apache.spark.ml.util.Identifiable
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}
import org.apache.spark.storage.StorageLevel

class neighborVectorDistance(override val uid: String) extends Transformer
  with HasSrcNodeIdCol with HasDstNodeIdCol with HasOutputNodeIdCol with HasOutputCoreIdCol
  with HasStorageLevel with HasPartitionNum with HasPSPartitionNum with HasBatchSize with HasUseBalancePartition
  with HasUseEdgeBalancePartition with HasBalancePartitionPercent with HasVecSep with HasItemSep with HasItemIdCol
  with HasVectorCol with HasQueryPath with HasItemIdx with HasVecIdx with HasQueryPartitionNum with HasDistanceFunction with HasOutputDistanceCol {

  def this() = this(Identifiable.randomUID("nodeDistance"))

  override def transform(dataset: Dataset[_]): DataFrame = {
    val rawEdges = NeighborDataOps.loadEdges(dataset, $(srcNodeIdCol), $(dstNodeIdCol), false, true).persist(StorageLevel.DISK_ONLY)
    val data = rawEdges.flatMap(f => Iterator(f._1, f._2))

    val (minId, maxId, numEdges) = Stats.summarize(rawEdges)
    println(s"minId=$minId maxId=$maxId numEdges=$numEdges level=${$(storageLevel)}")

    val graph = rawEdges.groupByKey($(partitionNum)).mapPartitionsWithIndex((index, adjTable) =>
      Iterator(neighborVectorDistanceGraphPartition.apply(index, adjTable, $(batchSize)))).persist($(storageLevel))
    graph.foreachPartition(_ => Unit) //trigger action

    println("start to run ps")
    PSContext.getOrCreate(SparkContext.getOrCreate())
    val modelContext = new ModelContext($(psPartitionNum), minId, maxId + 1, -1,
      "nodeDistance", SparkContext.getOrCreate().hadoopConfiguration)

    val ss = SparkSession.builder().getOrCreate()
    val queryDF = DataUtils.loadVectors(ss, $(queryPath), $(queryPartitionNum), $(itemSep), $(vecSep), $(itemIdx), $(vecIdx))
    val tQueryData = DataUtils.loadFromDF(queryDF, $(itemIdCol), $(vectorCol), $(vecSep), $(queryPartitionNum)).persist(StorageLevel.DISK_ONLY)

    //create ps model;init node label with node's id
    val model = neighborVectorDistancePSModel(modelContext, data, $(useBalancePartition), $(balancePartitionPercent))
    model.initQueries(tQueryData, $(batchSize))

    val distanceResult = graph.flatMap(_.process(model, $(disFunc)))

    val retRDD = distanceResult.map(f => Row.fromSeq(Seq[Any](f._1, f._2, f._3)))
    ss.createDataFrame(retRDD, transformSchema(dataset.schema))
  }

  override def transformSchema(schema: StructType): StructType = {
    StructType(Seq(
      StructField("src", LongType, nullable = false),
      StructField("dst", LongType, nullable = false),
      StructField("distance", FloatType, nullable = false)
    ))
  }

  override def copy(extra: ParamMap): Transformer = defaultCopy(extra)
}

