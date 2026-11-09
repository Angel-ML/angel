package com.tencent.angel.spark.ml.ann.partition

import com.tencent.angel.spark.ml.ann.index.{BruteForceIndex, Index}
import com.tencent.angel.spark.ml.ann.{ANNPSModel, DistanceFunctions}
import org.apache.spark.broadcast.Broadcast

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

class BruteForcePartition(sheetId: Int,
                          partIndex: Int,
                          val keys: Array[Long],
                          val vectors: Array[Array[Float]],
                          val bfModel: Index)
  extends ANNPartition(sheetId, partIndex) with Serializable {
  
  def process(psModel: ANNPSModel, queryIds: Broadcast[Array[Long]],
              topK: Int, batchSize: Int): Unit = {
    println(s"sheet $sheetId part $partIndex process ${queryIds.value.length} queries with ${bfModel.distanceFunction.getName}")
    
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
        searchRes.append((key, findNearest(value, topK)))
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
  
  override def setSheetId(id: Int): BruteForcePartition = {
    new BruteForcePartition(id, partIndex, keys, vectors, bfModel)
  }
  
  def findNearest(vec: Array[Float], k: Int): Array[(Long, Float)] = {
    implicit val ord: Ordering[(Long, Float)] = Ordering.by(_._2)
    // max heap
    val candidates = new mutable.PriorityQueue[(Long, Float)]()(ord)
    
    vectors.indices.foreach{ idx =>
      val distance = bfModel.distanceFunction.distance(vectors(idx), vec)
      if (candidates.size < k) {
        candidates.enqueue((keys(idx), distance))
      }
      else if (candidates.head._2 > distance) {
        candidates.dequeue()
        candidates.enqueue((keys(idx), distance))
      }
    }
    candidates.toArray.sortBy(_._2)
  }
}

object BruteForcePartition {
  def apply(index: Int, iter: Iterator[(Long, Array[Float])], distanceFunc: String): BruteForcePartition = {
    val partData = iter.toArray
    val keys = partData.map(_._1)
    val vectors = partData.map(_._2)
    
    val disFunc = DistanceFunctions.str2DistanceFunction(distanceFunc)
    val bfIndex = new BruteForceIndex()
    bfIndex.setDistanceFunc(disFunc)
    
    new BruteForcePartition(0, index, keys, vectors, bfIndex)
  }
}