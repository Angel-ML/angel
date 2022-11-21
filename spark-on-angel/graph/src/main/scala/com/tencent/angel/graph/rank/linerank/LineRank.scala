package com.tencent.angel.graph.rank.linerank

import com.tencent.angel.graph.common.param.ModelContext
import com.tencent.angel.spark.context.PSContext
import com.tencent.angel.graph.utils.params._
import org.apache.spark.SparkContext
import org.apache.spark.ml.Transformer
import org.apache.spark.ml.param.{FloatParam, IntParam, ParamMap}
import org.apache.spark.ml.util.Identifiable
import org.apache.spark.sql.types.{FloatType, LongType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Dataset, Row}

class LineRank(override val uid: String) extends Transformer
  with HasDstNodeIdCol with HasOutputNodeIdCol with HasOutputCentralityCol with HasSrcNodeIdCol
  with HasStorageLevel with HasPartitionNum with HasPSPartitionNum with HasBatchSize
  with HasWeightCol with HasIsWeighted with HasUseBalancePartition with HasBalancePartitionPercent {

  final val resetProb = new FloatParam(this, "resetProb", "resetProb")
  final val tol = new FloatParam(this, "tol", "tol")
  final val maxIter = new IntParam(this, "maxIter", "maxIter")
  final val numMsgPerBatch = new IntParam(this, "msgNumBatch", "msgNumBatch")

  final def setResetProb(prob: Float): this.type = set(resetProb, prob)
  final def setTol(t: Float): this.type = set(tol, t)
  final def setMaxIter(iter: Int): this.type  = set(maxIter, iter)
  final def setNumMsgPerBatch(batch: Int): this.type = set(numMsgPerBatch, batch)

  setDefault(resetProb, 0.15f)
  setDefault(tol, 0.01f)
  setDefault(maxIter, 200)
  setDefault(numMsgPerBatch, 4)

  def this() = this(Identifiable.randomUID("Linerank"))

  override def transform(dataset: Dataset[_]): DataFrame = {
    val edges = if ($(isWeighted)) {
      dataset.select($(srcNodeIdCol), $(dstNodeIdCol), $(weightCol)).rdd
        .map(row => (row.getLong(0), row.getLong(1), row.getFloat(2)))
        .filter(f => f._1 != f._2)
    } else {
      dataset.select($(srcNodeIdCol), $(dstNodeIdCol)).rdd
        .map(row => (row.getLong(0), row.getLong(1), 1.0f))
        .filter(f => f._1 != f._2)
    }

    val index = edges.flatMap(f => Array(f._1, f._2))
    val (minId, maxId, numEdges) = edges.mapPartitions(summarizeApplyOp).reduce(summarizeReduceOp)

    println(s"minId=$minId maxId=$maxId numEdges=$numEdges resetProb=${$(resetProb)}")

    // Start PS and init the model
    println("start to run ps")
    PSContext.getOrCreate(SparkContext.getOrCreate())

    // Create model
    val modelContext = new ModelContext($(psPartitionNum), minId, maxId + 1, -1,
      "linerank", SparkContext.getOrCreate().hadoopConfiguration)

    val model = LineRankPSModel(modelContext, index, $(useBalancePartition), $(balancePartitionPercent))

    var prev = edges.map(sd => (sd._2, (sd._1, sd._3))).groupByKey($(partitionNum))
      .mapPartitionsWithIndex((index, it) =>
        Iterator.single(LineRankGraphPartition.apply(model, index, it, $(resetProb), $(numMsgPerBatch))))

    prev.persist($(storageLevel))
    prev.foreachPartition(_ => Unit)
    var graph = prev.map(_.initOutDegs(model))
    graph.persist($(storageLevel))
    graph.count()
    prev.unpersist(true)
    prev = graph
    model.resetValues()

    var iter = 1
    var numActives = 0L
    do {
      prev.map(_.aggTargetIndices(model)).collect()
      graph = prev.map(_.batchedAggSrcIncidence(model, $(resetProb), $(tol), $(numMsgPerBatch)))
      graph.persist($(storageLevel))
      graph.count()
      prev.unpersist(true)
      prev = graph
      model.resetValues()
      numActives = prev.map(_.getNumActives).reduce(_ + _)
      println(s"iter=$iter numActives=$numActives")
      iter += 1
    } while (numActives > 0 && iter < $(maxIter))

    prev.map(_.calcNodeCentrality(model, $(numMsgPerBatch))).collect()
    val numNodes = model.numNodes()
    println(s"numNodes=$numNodes")

    val retRDD = prev.map(_.save(model)).flatMap(f => f._1.zip(f._2))
      .map { case (node, rank) => Row.fromSeq(Seq[Any](node, rank.toFloat))}

    val outputSchema = transformSchema(dataset.schema)
    dataset.sparkSession.createDataFrame(retRDD, outputSchema)
  }

  def summarizeApplyOp(iterator: Iterator[(Long, Long, Float)]): Iterator[(Long, Long, Long)] = {
    var minId = Long.MaxValue
    var maxId = Long.MinValue
    var numEdges = 0
    while (iterator.hasNext) {
      val entry = iterator.next()
      val (src, dst) = (entry._1, entry._2)
      minId = math.min(minId, src)
      minId = math.min(minId, dst)
      maxId = math.max(maxId, src)
      maxId = math.max(maxId, dst)
      numEdges += 1
    }

    Iterator.single((minId, maxId, numEdges))
  }

  def summarizeReduceOp(t1: (Long, Long, Long),
                        t2: (Long, Long, Long)): (Long, Long, Long) =
    (math.min(t1._1, t2._1), math.max(t1._2, t2._2), t1._3 + t2._3)

  override def transformSchema(schema: StructType): StructType = {
    StructType(Seq(
      StructField(s"${$(outputNodeIdCol)}", LongType, nullable = false),
      StructField(s"${$(outputCentralityCol)}", FloatType, nullable = false)
    ))
  }

  override def copy(extra: ParamMap): Transformer = defaultCopy(extra)

}
