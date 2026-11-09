package com.tencent.angel.spark.ml.ann.partition

import com.tencent.angel.spark.ml.ann.index.IndexGraph
import com.tencent.angel.spark.ml.ann.{ANNPSModel, IVFHnswPSModel}
import org.apache.spark.broadcast.Broadcast

import scala.collection.mutable.ArrayBuffer

abstract class IVFHnswGraphPartition(sheetId: Int,
                                     partIndex: Int,
                                     val graphKeys: Array[Long],
                                     val graphIndexModel: IndexGraph)
  extends IVFHnswAbsPartition(sheetId, partIndex) with Serializable {

  def buildGraph(): IVFHnswGraphPartition

  override def process(psModel: IVFHnswPSModel, queryIds: Broadcast[Array[Long]],
                       topK: Int, batchSize: Int): Unit = {
    println(s"sheet $sheetId part $partIndex process ${queryIds.value.length} queries with ${graphIndexModel.distanceFunction.getName}")

    var batchCnt = 0
    var batchTime = System.currentTimeMillis()
    queryIds.value.sliding(batchSize, batchSize).foreach { partQueries =>
      val t1 = System.currentTimeMillis()
      val queries = psModel.getQueries(partQueries)

      val t2 = System.currentTimeMillis()
      val iter = queries.entrySet().iterator()
      val searchRes = new ArrayBuffer[(Long, Array[(Long, Float)])]()
      while (iter.hasNext) {
        val entry = iter.next()
        val key = entry.getKey
        val value = entry.getValue
        val neighbors = this.findNearest(value, topK)
        if (neighbors.nonEmpty) {
          neighbors.foreach(a => searchRes += a)
        }
      }

      val t3 = System.currentTimeMillis()

      // push to ps
      psModel.updateSearchResult(searchRes.toArray, topK)
      val t4 = System.currentTimeMillis()

      println(s"part $partIndex, pull cost: ${t2 - t1} ms, find part nearest cost: ${t3 - t2} ms, push and merge on ps cost: ${t4 - t3} ms.")

      batchCnt += 1
      if (batchCnt % 100 == 0) {
        println(s"part $partIndex, $batchCnt batches done, cost ${(System.currentTimeMillis() - batchTime) / 1000.0} s")
        batchTime = System.currentTimeMillis()
      }
    }
  }

  override def findNearest(vecs: (Array[Long], Array[Array[Float]]), k: Int): Array[(Long, Array[(Long, Float)])] = {
    val labelsArray = vecs._1
    val vecsArray = vecs._2
    Array.range(0, labelsArray.length).map { i =>
      (labelsArray(i), this.graphIndexModel.findNearest(vecsArray(i), k).map(t => (t.getItem.getId, t.getDistance)).toArray)
    }
  }

  def processApproximation(psModel: ANNPSModel, topK: Int, batchSize: Int): Unit = {
    var batchCnt = 0
    var batchTime = System.currentTimeMillis()
    graphKeys.sliding(batchSize, batchSize).foreach { partQueries =>
      val t1 = System.currentTimeMillis()
      val searchRes = new ArrayBuffer[(Long, Array[(Long, Float)])]()
      for (qid <- partQueries) {
        val neighbors = graphIndexModel.findNeighbors(qid, topK)
        if (neighbors.nonEmpty) {
          val resK = neighbors.map(t => (t.getItem.getId, t.getDistance)).toArray
          searchRes += ((qid, resK))
        }
      }
      val t2 = System.currentTimeMillis()

      // push to ps
      psModel.updateSearchResult(searchRes.toArray, topK)
      val t3 = System.currentTimeMillis()

      println(s"part $partIndex, find part nearest cost: ${t2 - t1} ms, push and merge on ps cost: ${t3 - t2} ms.")

      batchCnt += 1
      if (batchCnt % 100 == 0) {
        println(s"part $partIndex, $batchCnt batches done, cost ${(System.currentTimeMillis() - batchTime) / 1000.0} s")
        batchTime = System.currentTimeMillis()
      }
    }
  }
}
