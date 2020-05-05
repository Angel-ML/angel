package com.tencent.angel.graph.feature.interactRate

import com.tencent.angel.spark.context.PSContext
import com.tencent.angel.graph.params._
import org.apache.spark.ml.Transformer
import org.apache.spark.ml.util.Identifiable
import org.apache.spark.SparkContext
import org.apache.spark.ml.param.ParamMap
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Dataset, Row}
import org.apache.spark.storage.StorageLevel

class InteractRate(override val uid: String) extends Transformer
  with HasWeightCol with HasSrcNodeIdCol with HasDstNodeIdCol
  with HasOutputNodeIdCol with HasOutputCoreIdCol
  with HasIsWeighted with HasPartitionNum with HasPSPartitionNum
  with HasStorageLevel with HasBatchSize with HasPullBatchSize
  with HasBufferSize with HasUseBalancePartition {

  def this() = this(Identifiable.randomUID("InteractRate"))

  override def transform(dataset: Dataset[_]): DataFrame = {
    //read edges
    val edges = if ($(isWeighted)) {
      dataset.select($(srcNodeIdCol), $(dstNodeIdCol), $(weightCol)).rdd
        .filter(row => !row.anyNull)
        .map(row => (row.getLong(0), row.getLong(1), row.getFloat(2)))
        .filter(e => e._1 != e._2)
    } else {
      dataset.select($(srcNodeIdCol), $(dstNodeIdCol)).rdd
        .filter(row => !row.anyNull)
        .map(row => (row.getLong(0), row.getLong(1), 1.0f))
        .filter(e => e._1 != e._2)
    }
    
    //edges's storageLevel choose  Disk_Only
    edges.persist(StorageLevel.DISK_ONLY)

    val maxId = edges.map(e => math.max(e._1, e._2)).max() + 1
    val minId = edges.map(e => math.min(e._1, e._2)).min()
    val nodes = edges.flatMap(e => Iterator(e._1, e._2))
    val numEdges = edges.count()

    println(s"minId=$minId maxId=$maxId numEdges=$numEdges level=${$(storageLevel)}")

    // Start PS and init the model
    println("start to run ps")
    PSContext.getOrCreate(SparkContext.getOrCreate())

    //    graph.persist($(storageLevel))
    //    graph.count()
//     val model = InteractPSModel.fromMinMax(minId, maxId, nodes, $(psPartitionNum), $(useBalancePartition))

    // true for origin directed graph,
    // false for direction reversed graph, out-degree equals to in-degree of orginal graph
    var graph = edges.flatMap { case (srcId, dstId, weight) => Iterator((srcId, (dstId, (true, weight))), (dstId, (srcId, (false, weight)))) }
      .groupByKey($(partitionNum))
      .mapPartitionsWithIndex((index, adjTable) => Iterator(InteractGraphPartition.apply(index, adjTable)))
    graph.persist($(storageLevel))
    graph.foreachPartition(_ => Unit)

    val res = graph.map(_.process())
    res.persist()
    res.count()

    val retRDD_ = res.flatMap { case (nodes, cores) =>  nodes.zip(cores)}
    println(s"retRDD_ length: ${retRDD_.count()}")

    // whether inRate or symRate can be decided by filter
    val retRDD = retRDD_.filter(x => x._2 != null)
    println(s"retRDD length: ${retRDD.count()}")

    val ret = retRDD.map(r => Row.fromSeq(Seq[Any](r._1, r._2._1, r._2._2)))

    dataset.sparkSession.createDataFrame(ret, transformSchema(dataset.schema))

  }


  override def transformSchema(schema: StructType): StructType = {
    StructType(Seq(
      StructField(s"${
        $(outputNodeIdCol)
      }", LongType, nullable = false),
      StructField(s"${
        "InDegreeaRate"
      }", FloatType, nullable = false),
      StructField(s"${
        "SymmetryRate"
      }", FloatType, nullable = false)
    ))
  }

  override def copy(extra: ParamMap): Transformer = defaultCopy(extra)
}

