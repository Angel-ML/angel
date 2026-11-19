package com.tencent.angel.spark.ml.graph.algo.rank.toprelation

import com.tencent.angel.graph.common.param.ModelContext
import com.tencent.angel.spark.context.PSContext
import com.tencent.angel.spark.ml.graph.utils.params._
import com.tencent.angel.spark.ml.graph.algo.rank.multiAttrHIndex.MultiAttrHIndexPSModel
import org.apache.spark.SparkContext
import org.apache.spark.ml.Transformer
import org.apache.spark.ml.param.ParamMap
import org.apache.spark.ml.util.Identifiable
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Dataset, Row}
import org.apache.spark.storage.StorageLevel

// requested songs from friends
// requested songs from close friends
// most requested songs from friends
// most requested songs from close friends

class TopCandidate(override val uid: String) extends Transformer
  with HasWeightCol with HasSrcNodeIdCol with HasDstNodeIdCol
  with HasOutputNodeIdCol with HasOutputCoreIdCol
  with HasIsWeighted with HasPartitionNum with HasPSPartitionNum
  with HasStorageLevel with HasBatchSize with HasUseBalancePartition
  with HasNodeAttrPath with HasTopK with HasUnDirected with HasMinNodeId with HasMaxNodeId {

  def this() = this(Identifiable.randomUID("TopCandidate"))

  var nodeAttrs: RDD[(Long, Array[Float])] = _

  def setNodeAttr(nodeAttr: RDD[(Long, Array[Float])]): Unit = {
    this.nodeAttrs = nodeAttr
  }

  override def transform(dataset: Dataset[_]): DataFrame = {
    //read edges

    val edges = TopCandidate.loadEdges(dataset, $(srcNodeIdCol), $(dstNodeIdCol), $(isWeighted), $(weightCol))
    edges.persist(StorageLevel.DISK_ONLY)
    println(s"======sample edges======")
    println(edges.take(10).mkString(","))

    //    val stats = TopCandidate.stats(edges: RDD[(Long, (Long, Float))])

    val nodes = edges.flatMap(e => Iterator(e._1, e._2._1))
    //    val maxId = stats._2 + 1
    //    val minId = stats._1
    //    val numEdges = stats._3

    val minId = $(minNodeId)
    val maxId = $(maxNodeId) + 1

    //    println(s"minId=$minId maxId=$maxId numEdges=$numEdges level=${$(storageLevel)}")

    //     Start PS and init the model
    println("start to run ps")
    PSContext.getOrCreate(SparkContext.getOrCreate())

    val modelContext = new ModelContext($(psPartitionNum), minId, maxId + 1, -1,
      "attr", SparkContext.getOrCreate().hadoopConfiguration)
    val model = MultiAttrHIndexPSModel.fromMinMax($(batchSize), modelContext, nodes, $(useBalancePartition))

    //    val numNodesWithAttrs = nodeAttrs.count()
    //    println(s"numNodes with attrs=$numNodesWithAttrs")

    val num = model.initAttributes(nodeAttrs.filter(x => x._1 < maxId).filter(x => x._1 >= minId))
    println(s"Init $num nodes attrs.")

    model.checkpoint()
    println(s"checkpoint finished.")

    val graph = if ($(unDirected)) {
      edges.flatMap { case (srcId, (dstId, weight)) => Iterator((srcId, (dstId, weight)), (dstId, (srcId, weight))) }
        .groupByKey($(partitionNum))
        .mapPartitionsWithIndex((index, adjTable) => Iterator(TopCandidateGraphPartition.apply(index, adjTable)))
    } else {
      edges.groupByKey($(partitionNum))
        .mapPartitionsWithIndex((index, adjTable) => Iterator(TopCandidateGraphPartition.apply(index, adjTable)))
    }

    graph.persist($(storageLevel))
    graph.foreachPartition(_ => Unit)

    val result = graph.map(_.process(model, $(batchSize), $(topK)))
    graph.persist($(storageLevel))
    //    result.count()

    //    val res =graph.map(_.save()).cache()

    val retRDD = result.flatMap{ case (nodes, cores) => nodes.zip(cores)}
      .map(r => Row.fromSeq(Seq[Any](r._1, r._2._1, r._2._2, r._2._3, r._2._4)))
    dataset.sparkSession.createDataFrame(retRDD, transformSchema(dataset.schema))

  }

  override def transformSchema(schema: StructType): StructType = {
    StructType(Seq(
      StructField(s"${
        $(outputNodeIdCol)
      }", LongType, nullable = false),
      StructField(s"${
        "topCandidate1"
      }", StringType, nullable = false),
      StructField(s"${
        "topCandidate2"
      }", StringType, nullable = false),
      StructField(s"${
        "topCandidate3"
      }", StringType, nullable = false),
      StructField(s"${
        "topCandidate4"
      }", StringType, nullable = false)
    ))
  }

  override def copy(extra: ParamMap): Transformer = defaultCopy(extra)

}

object TopCandidate {
  def loadEdges(dataset: Dataset[_], srcNodeIdCol: String, dstNodeIdCol: String,
                isWeighted: Boolean, weightIdCol: String): RDD[(Long, (Long, Float))] = {
    if (isWeighted) {
      dataset.select(srcNodeIdCol, dstNodeIdCol, weightIdCol).rdd.mapPartitions { iter =>
        iter.flatMap { row =>
          if (row.getLong(0) == row.getLong(1) || row.getFloat(2) == 0)
            Iterator.empty
          else Iterator.single(row.getLong(0), (row.getLong(1), row.getFloat(2)))
        }
      }
    } else {
      dataset.select(srcNodeIdCol, dstNodeIdCol)}.rdd.mapPartitions { iter =>
      iter.flatMap { row =>
        if (row.getLong(0) == row.getLong(1))
          Iterator.empty
        else
          Iterator.single(row.getLong(0), (row.getLong(1), 1.0f))
      }
    }
  }

  def stats(edges: RDD[(Long, (Long, Float))]): (Long, Long, Long) = {
    edges.mapPartitions { iter =>
      var min = Long.MaxValue
      var max = Long.MinValue
      var numEdges = 0L
      iter.foreach { case (src, (dst, w)) =>
        if (src < dst) {
          min = math.min(min, src)
          max = math.max(max, dst)
        } else {
          min = math.min(min, dst)
          max = math.max(max, src)
        }
        numEdges += 1
      }
      Iterator.single((min, max, numEdges))
    }.reduce { case (a, b) =>
      (a._1 min b._1, a._2 max b._2, a._3 + b._3)
    }
  }
}
