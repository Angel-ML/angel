package com.tencent.angel.spark.ml.ann

import com.tencent.angel.graph.common.param.ModelContext
import com.tencent.angel.spark.context.PSContext
import com.tencent.angel.spark.ml.ann.params._
import com.tencent.angel.spark.ml.ann.partition.IVFHnswAbsPartition
import com.tencent.angel.spark.ml.ann.utils.DataUtils
import com.tencent.angel.spark.ml.ann_core.{IVFHnswVector, NeighborDistance}
import com.tencent.angel.spark.ml.graph.utils.params._
import org.apache.spark.SparkContext
import org.apache.spark.ml.param.{ParamMap, Params}
import org.apache.spark.ml.util.Identifiable
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.{LongType, StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}
import org.apache.spark.storage.StorageLevel

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.util.Random

class IVFHnswModel[T <: IVFHnswAbsPartition](override val uid: String) extends Serializable with HasTopK with HasTopKInCluster with HasVecSep
  with HasItemSep with HasItemIdCol with HasVectorCol with HasDistanceFunction with HasPartitionNum
  with HasPSPartitionNum with HasStorageLevel with HasQueryPath with HasIsInternalQuery with HasBatchSize
  with HasMaxItemCount with HasItemIdx with HasVecIdx with HasQueryPartitionNum
  with HasUseBalancePartition with HasBalancePartitionPercent with HasSheetsNum with HasClusterIdx with HasClusterIDCol with HasQueryBatchSize {

  def this() = this(Identifiable.randomUID("IVFHNSWModel"))

  var ss: SparkSession = _
  var KNN: Int = _
  var KNNInCluster: Int = _
  var data: RDD[(Long, Array[Float])] = _
  var queryIds: RDD[Long] = _
  var psModel: IVFHnswPSModel = _
  var annParts: RDD[T] = _
  var querySheets: Array[RDD[Long]] = _
  var partitionSheets: RDD[(Int, T)] = _
  var oriQueryRDD: RDD[(Long, Array[Float])] = _

  def start(dataset: Dataset[_]): Unit = {
    this.ss = dataset.sparkSession
    this.KNN = $(topK) //if ($(isInternal)) $(topK) else $(topK) + 1
    this.KNNInCluster = $(topKInCluster)

    //load cluster centers ;cluster id |cluster verctor
    data = DataUtils.loadFromDF(dataset, $(itemIdCol), $(vectorCol), $(vecSep), $(partitionNum))
    data.persist($(storageLevel))
    val numVectors = data.count()

    println(s"$numVectors items in total...")
    val sc = SparkContext.getOrCreate()

    var (queryData, tQueryIds, queryRDD) = {
      assert($(queryPath) != null, s"query path is required.")
      val queryDF = DataUtils.loadVectorsWithClusterIDMultilevel(sc, $(queryPath), $(queryPartitionNum), $(itemSep), $(vecSep), $(itemIdx), $(vecIdx), $(clusterIdx))
      val tQueryData = DataUtils.loadFromDFWithClusterID(queryDF, $(itemIdCol), $(vectorCol), $(clusterIDCol), $(vecSep), $(queryPartitionNum))
      val queryRDD = DataUtils.loadOriQueryFromDF(queryDF, $(itemIdCol), $(vectorCol), $(vecSep), $(queryPartitionNum))
      val tQIdsRDD = tQueryData.keys
      println(s"load query ids and vectors from ${$(queryPath)}. ")
      (tQueryData, tQIdsRDD, queryRDD)
    }


    this.queryIds = tQueryIds
    this.oriQueryRDD = queryRDD

    val queryCnt = queryData.count()
    if (queryCnt < $(queryPartitionNum)) {
      println(s"find ${queryCnt} queries, which is less than query part num ${
        ${
          queryPartitionNum
        }
      }, repartition queries to ${queryCnt} parts")
      setQueryPartitionNum(queryCnt.toInt)
      println(s"new query partition num is ${$(queryPartitionNum)}")
      queryData = queryData.repartition($(queryPartitionNum))
    }
    queryData.persist(StorageLevel.DISK_ONLY)
    queryData.count()


    this.oriQueryRDD.persist(StorageLevel.DISK_ONLY)
    println(s"total ${this.oriQueryRDD.count()} oriQueryDF loaded.")


    this.queryIds.persist($(storageLevel))
    println(s"total ${this.queryIds.count()} queries loaded.")

    println(s"process with ${$(disFunc)} function")

    val minId = queryIds.min()
    val maxId = queryIds.max()

    println(s"queryMinId: $minId queryMaxId: $maxId")
    println("start to run ps")
    PSContext.getOrCreate(SparkContext.getOrCreate())

    val modelContext = new ModelContext($(psPartitionNum), minId, maxId + 1, -1,
      SparkContext.getOrCreate().hadoopConfiguration)
    this.psModel = IVFHnswPSModel.apply(modelContext, this.data, $(useBalancePartition), $(balancePartitionPercent))


    println("start init ps...")
    val timeInitPS = System.currentTimeMillis()
    // init queries matrix
    this.psModel.initQueries(queryData)
    queryData.unpersist(blocking = false)


    println(s"init ps cost: ${(System.currentTimeMillis() - timeInitPS) / 1000.0} s")

    // checkpoint for vectors and queries
    println("save checkpoint...")
    val timeWriteCheckpoint = System.currentTimeMillis()
    psModel.checkpoint(0, Array("queryVector", "searchRes"))
    println(s"write checkpoint cost: ${(System.currentTimeMillis() - timeWriteCheckpoint) / 1000.0} s")

  }

  def setANNPartsWithSheets(parts: RDD[T]): Unit = {
    this.makeSheets()
    this.partitionSheets = parts.map(p => (0, p))
    for (i <- 1 until $(sheetsNum)) {
      this.partitionSheets = this.partitionSheets.union(parts.map(p => (i, p.setSheetId(i).asInstanceOf[T])))
    }
    this.partitionSheets.persist($(storageLevel))
    println(s"origin parts num: ${parts.count()}, ${this.partitionSheets.count()} parts of ${$(sheetsNum)} sheets")
    this.data.unpersist(blocking = false)
  }

  def makeSheets(): Unit = {
    this.querySheets = this.queryIds.randomSplit(Array.fill($(sheetsNum))(1.0))
  }

  def processANNParts(): Unit = {
    assert(this.partitionSheets != null, s"need to set ann partition sheets")

    val brQIdsArray = this.querySheets.map(sheet => SparkContext.getOrCreate().broadcast(sheet.collect()))

    println(s"start to process nearest neighbors")
    val timeFindKNN = System.currentTimeMillis()
    this.partitionSheets.foreach(sheet => sheet._2.process(psModel, brQIdsArray(sheet._1), KNN, $(batchSize)))
    this.partitionSheets.unpersist(blocking = false)
    println(s"find knn with top ${$(topK)} cost: ${(System.currentTimeMillis() - timeFindKNN) / 1000.0} s")
  }


  def processOriQueryDFParts(disFunc: String): Unit = {
    println(s"start to search nearest neighbors in cluster =====")
    val timeStart = System.currentTimeMillis()
    val distanceFunction = DistanceFunctions.str2DistanceFunction(disFunc)

    val retRDDTmp = IVFHnswModel.save2(this.psModel, this.oriQueryRDD, this.KNN, $(queryBatchSize))
      .flatMap(a => a._3.map(t => (t.getNeighbor, (a._1, a._2)))).groupByKey().flatMap { item =>
      val key = item._1
      val random = new Random(10)
      item._2.toArray.grouped(2500).map { i => (random.nextInt()+"_"+key, i) }
    }.cache()
    retRDDTmp.checkpoint()

    println(retRDDTmp.count()) //?????
    val retRDD = retRDDTmp.repartition($(queryPartitionNum)).persist(StorageLevel.DISK_ONLY)
    retRDD.checkpoint()
    println(retRDD.count()) //?????


    retRDD.foreachPartition { part1 =>

      part1.sliding($(batchSize), $(batchSize)).foreach { part =>

        val t1 = System.currentTimeMillis()
        val partClusterID = part.map(_._1.split("_")(1).toLong).toArray
        val partData = part.map(_._2).toArray
        val queries = this.psModel.getQueries(partClusterID.distinct)

        val searchRes = new ArrayBuffer[(Long, Array[(Long, Float)])]()

        Array.range(0, partClusterID.length).foreach { i =>
          val clusterID = partClusterID(i)
          val data = partData(i)
          val queryVector = data.map(_._2)
          val queryLabels = data.map(_._1)

          val clusterData = queries.getOrDefault(clusterID, null)

          if (clusterData != null) {
            val t11 = System.currentTimeMillis()
            val clusterLabels = clusterData._1
            //println(clusterLabels.mkString(","))
            val clusterVectors = clusterData._2

            for (qI <- queryVector.indices) {
              implicit val ord: Ordering[NeighborDistance] = Ordering.by(_.getDistance)
              val candidates = new mutable.PriorityQueue[NeighborDistance]()

              for (j <- clusterLabels.indices) {
                val label = clusterLabels(j)
                val vector = clusterVectors(j)
                if (queryLabels(qI) != label) {
                  val distance = distanceFunction.distance(vector, queryVector(qI))
                  val idWithDistancePair = new NeighborDistance(label, distance)
                  candidates.enqueue(idWithDistancePair)
                  if (candidates.size > this.KNNInCluster) {
                    candidates.dequeue()
                  }
                }
              }
              val foundKeysDis = new ArrayBuffer[(Long, Float)]()
              while (candidates.nonEmpty && foundKeysDis.length < this.KNNInCluster) {
                val pair = candidates.dequeue()
                foundKeysDis.append((pair.getNeighbor, pair.getDistance))
              }
              searchRes.append((queryLabels(qI), foundKeysDis.toArray))
            }

            val t22 = System.currentTimeMillis()
            println(s"$clusterID clusterLabels ${clusterLabels.size}  data number ${data.size} get nearest in clusters cost: ${t22 - t11} ms")
          }
          else {
            println("the clusterData is null for cluster id " + clusterID)
          }
        }
        val t2 = System.currentTimeMillis()
        println(s"part ${partData.size} get nearest in clusters cost: ${t2 - t1} ms")
        psModel.updateSearchResultInCluster(searchRes.toArray, this.KNNInCluster)
      }
    }
    println(s"search nearest neighbors in cluster cost: ${(System.currentTimeMillis() - timeStart) / 1000.0} s")
  }


  def saveKNNResults(): DataFrame = {
    println(s"start to make saving data")
    val timeMakeSaving = System.currentTimeMillis()

    val retRDD = IVFHnswModel.save(this.psModel, this.oriQueryRDD, this.KNNInCluster, $(queryBatchSize))
      .map(a => (a._1, a._2.map(t => s"${t.getNeighbor}:${t.getDistance}").mkString(" ")))
      .map(row => Row.fromSeq(Seq[Any](row._1, row._2)))
    println(s"make saving data cost: ${System.currentTimeMillis() - timeMakeSaving} ms")

    val schema = StructType(Seq(
      StructField("query", LongType, nullable = false),
      StructField("nbr:dis", StringType, nullable = false)))

    this.ss.createDataFrame(retRDD, schema)
  }

  override def copy(extra: ParamMap): Params = defaultCopy(extra)
}


object IVFHnswModel {
  def save(psModel: IVFHnswPSModel, oriQueryRDD: RDD[(Long, Array[Float])], topK: Int,
           batchSize: Int): RDD[(Long, Array[NeighborDistance])] = {
    oriQueryRDD.mapPartitions { partQ =>
      val partQIds = partQ.toArray.map(_._1)
      var batchCnt = 0
      var batchTime = System.currentTimeMillis()
      partQIds.sliding(batchSize, batchSize).flatMap { part =>
        val partRes = psModel.getNearestNeighbors(part, topK)
        val iter = partRes.entrySet().iterator()
        val savedPart = new ArrayBuffer[(Long, Array[NeighborDistance])]()
        while (iter.hasNext) {
          val ks = new ArrayBuffer[Long]()
          val ds = new ArrayBuffer[Float]()
          val entry = iter.next()
          val key = entry.getKey
          val value = entry.getValue
          savedPart.append((key, value))
        }
        batchCnt += 1
        if (batchCnt % 500 == 0) {
          println(s"$batchCnt batches pulled for saving, cost ${(System.currentTimeMillis() - batchTime) / 1000.0} s")
          batchTime = System.currentTimeMillis()
        }
        savedPart.toIterator
      }
    }
  }

  def save2(psModel: IVFHnswPSModel, oriQueryRDD: RDD[(Long, Array[Float])], topK: Int,
            batchSize: Int): RDD[(Long, Array[Float], Array[NeighborDistance])] = {
    oriQueryRDD.mapPartitions { partQ =>
      var batchCnt = 0
      var batchTime = System.currentTimeMillis()
      partQ.sliding(batchSize, batchSize).flatMap { part =>
        val partID = part.map(_._1).toArray
        val partVector = part.map(_._2).toArray
        val partRes = psModel.getNearestClusters(partID, topK)
        val savedPart = new ArrayBuffer[(Long, Array[Float], Array[NeighborDistance])]()

        Array.range(0, partID.length).foreach { i =>
          val key = partID(i)
          val value = partRes.get(key)
          savedPart.append((key, partVector(i), value))
        }

        batchCnt += 1
        if (batchCnt % 500 == 0) {
          println(s"$batchCnt batches pulled for saving, cost ${(System.currentTimeMillis() - batchTime) / 1000.0} s")
          batchTime = System.currentTimeMillis()
        }
        savedPart.toIterator
      }
    }
  }

}
