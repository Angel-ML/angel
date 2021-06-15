package com.tencent.angel.graph.rank.swing

import com.tencent.angel.graph.common.param.ModelContext
import com.tencent.angel.graph.data.neighbor.NeighborDataOps
import com.tencent.angel.graph.model.neighbor.simple.SimpleNeighborTableModel
import com.tencent.angel.graph.utils.params._
import com.tencent.angel.spark.context.PSContext
import it.unimi.dsi.fastutil.longs.Long2ObjectOpenHashMap
import org.apache.spark.SparkContext
import org.apache.spark.ml.Transformer
import org.apache.spark.ml.param.ParamMap
import org.apache.spark.ml.util.Identifiable
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Dataset, Row}
import org.apache.spark.storage.StorageLevel
import org.apache.spark.util.collection.OpenHashSet

import scala.collection.mutable

// input: user->item pairs
// calculating similarities especially for less popular items
// normally no super item exists since topK items will be filtered

// item range: [ItemFrom, ItemTo]

class Swing(override val uid: String) extends Transformer
  with HasWeightCol with HasSrcNodeIdCol with HasDstNodeIdCol
  with HasOutputNodeIdCol with HasOutputCoreIdCol
  with HasIsWeighted with HasPartitionNum with HasPSPartitionNum
  with HasStorageLevel with HasBatchSize
  with HasPullBatchSize with HasDelta with HasBeta with HasGamma
  with HasSuperItemThreshold {

  def this() = this(Identifiable.randomUID("SwingV6"))
  var itemFrom: Int = 0
  var itemTo: Int = 0
  var superItemPairBatch: Int = 0
  def setItemFrom(in: Int): Unit = { this.itemFrom = in }
  def setItemTo(in: Int): Unit = { this.itemTo = in }
  def setSuperItemPairBatch(in: Int): Unit = { this.superItemPairBatch = in }

  override def transform(dataset: Dataset[_]): DataFrame = {

    val sc = dataset.sparkSession.sparkContext

    //read edges
    // user -> item pairs, no need to filter edges with the same id
    val edges = NeighborDataOps.loadEdges(dataset, ${srcNodeIdCol}, ${dstNodeIdCol},
      false, false)

    edges.persist(StorageLevel.DISK_ONLY)

    println(s"======sample edges======")
    println(edges.take(10).mkString(","))

    val itemHashSet = new OpenHashSet[Long]()

    assert(itemFrom >= 0 && itemTo >= 0 , s"error: invalid itemFrom or itemTo param, must be over or equal to 0.")
    if (itemTo > itemFrom) {
      if (itemTo - itemFrom > 100000000) {
        println(s"Over 100 million items need to be concerned according to the input params.")
      }
      val validItems = edges.map { case (_, item) => (item, 1)}
        .reduceByKey((a, b) => a + b, ${partitionNum})
        .sortBy(_._2, false)
        .zipWithIndex()
        .filter(_._2 >= itemFrom)
        .filter(_._2 < itemTo)
        .map(_._1._1)
        .collect()

      validItems.foreach { item => itemHashSet.add(item)}
    }

    // push userItem neighbor table to ps
    val userItemNeighborTable = if (itemHashSet.size > 0)
      SwingOperator.userItem2NeighborTable(edges, ${partitionNum}, itemHashSet)
    else SwingOperator.userItem2NeighborTable(edges, ${partitionNum})

    val (minUserId, maxUserId, numUsers, numUserItemEdges, maxUserItemDegree, minUserItemDegree) =
      SwingOperator.stats(userItemNeighborTable)
    println(s"maxUserId: $maxUserId, minUserId: $minUserId, " +
      s"numUsers: $numUsers, numUserItemEdges: $numUserItemEdges, " +
      s"maxUserItemDegree: $maxUserItemDegree, " +
      s"minUserItemDegree: $minUserItemDegree")

    userItemNeighborTable.persist($(storageLevel))

    // discard certain items
    val itemUserNeighborTable = if (itemHashSet.size > 0) {
      SwingOperator.userItem2NeighborTable(edges.map(e => (e._2, e._1)), ${partitionNum})
        .filter(x => itemHashSet.contains(x._1))
    } else {
      SwingOperator.userItem2NeighborTable(edges.map(e => (e._2, e._1)), ${partitionNum})
    }
    val (minItemId, maxItemId, numItems, numItemUserEdges, maxItemUserDegree, minItemUserDegree) =
      SwingOperator.stats(itemUserNeighborTable)
    println(s"maxItemId: $maxItemId, minItemId: $minItemId, " +
      s"numItems: $numItems, numItemUserEdges: $numItemUserEdges, " +
      s"maxItemUserDegree: $maxItemUserDegree, " +
      s"minItemUserDegree: $minItemUserDegree")

    // Start PS and init the model
    println("start to run ps")
    val beforeStartPS = System.currentTimeMillis()
    PSContext.getOrCreate(SparkContext.getOrCreate())
    println(s"Starting ps cost ${System.currentTimeMillis() - beforeStartPS} ms")

    println(s"push neighbor tables to ps")
    val initTableStartTime = System.currentTimeMillis()
    val modelContext = new ModelContext(${psPartitionNum}, minUserId, maxUserId + 1, numUsers,
      "neighborTable", sc.hadoopConfiguration)
    val psModel = new SimpleNeighborTableModel(modelContext)
    psModel.init()
    userItemNeighborTable.mapPartitions { iter =>
      iter.sliding(${batchSize}, ${batchSize}).map(pairs => psModel.initNeighbors(pairs))
    }.count()
    println(s"initializing the neighbor table costs ${System.currentTimeMillis() - initTableStartTime} ms")
    val cpTableStartTime = System.currentTimeMillis()
    psModel.checkpoint()
    println(s"checkpoint of neighbor table costs ${System.currentTimeMillis() - cpTableStartTime} ms")

    // start calculating
    val result1 = itemUserNeighborTable.mapPartitionsWithIndex { case (partId, iter) =>
      SwingOperator.runNeighborPartition(partId, iter, psModel, ${pullBatchSize}, ${delta}, ${beta}, ${gamma}, ${superItemThreshold})
    }

    // ----------- super item pairs --------------------
    result1.persist(StorageLevel.MEMORY_ONLY)
    val superItemPairs = result1.filter(_._3 < 0).collect()
    println(s"num of superSets: ${superItemPairs.length}")
    val superItems = new mutable.HashSet[Long]()
    superItemPairs.foreach { e => superItems.add(e._1)}
    val superItemUsers = new Long2ObjectOpenHashMap[Array[Long]]()
    itemUserNeighborTable.filter(x => superItems.contains(x._1)).collect()
      .foreach { case (item, users) => superItemUsers.put(item, users)}

    // start calculating super sets by batch
    val superResult = SwingOperator.calcSuperItemPairs(superItemPairs, superItemPairBatch, psModel,
      ${partitionNum}, superItemUsers, ${delta}, ${beta}, ${gamma})

//    superResult.foreach(println)
    val superResultRdd = SparkContext.getOrCreate.parallelize(superResult.toArray.toSeq, 1)
    val similarities = result1.union(superResultRdd).filter(x => !(x._3 < 0f))

    val retRDD = similarities.map { case (itemI, itemJ, score) =>
      Row.fromSeq(Seq[Any](itemI, itemJ, score))
    }

    dataset.sparkSession.createDataFrame(retRDD, transformSchema(dataset.schema))

  }

  override def transformSchema(schema: StructType): StructType = {
    StructType(Seq(
      StructField(s"${
        "itemI"
      }", LongType, nullable = false),
      StructField(s"${
        "itemJ"
      }", LongType, nullable = false),
      StructField(s"${
        "score"
      }", FloatType, nullable = false)
    ))
  }

  override def copy(extra: ParamMap): Transformer = defaultCopy(extra)

}

object Swing {

}
