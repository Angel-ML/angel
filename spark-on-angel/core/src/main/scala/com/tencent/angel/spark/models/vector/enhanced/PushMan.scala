package com.tencent.angel.spark.models.vector.enhanced

import com.tencent.angel.spark.client.PSClient
import com.tencent.angel.spark.models.vector.PSVector

import scala.collection.concurrent.TrieMap

object PushMan {

  import MergeType._
  def vectorOps = PSClient.instance().vectorOps

  val mergeCache = new TrieMap[(PSVector, MergeType), Array[Double]]()


  def getFromIncrementCache(vector: PSVector): Array[Double] = {
    getCachedArray(vector, INCREMENT)
  }

  def getFromMaxCache(vector: PSVector): Array[Double] = {
    getCachedArray(vector, MAX)
  }

  def getFromMinCache(vector: PSVector): Array[Double] = {
    getCachedArray(vector, MIN)
  }

  private def getCachedArray(vector: PSVector, mergeType: MergeType): Array[Double] = {
    mergeType match {
      case INCREMENT =>
        cacheGet(vector, mergeType, 0.0)
      case MAX =>
        cacheGet(vector, mergeType, Double.MinValue)
      case MIN =>
        cacheGet(vector, mergeType, Double.MaxValue)
    }
  }

  private def cacheGet(vector: PSVector, mergeType: MergeType, defaultValue: Double): Array[Double] = {
    mergeCache.synchronized {
      if (!mergeCache.contains((vector, mergeType))) {
        mergeCache.put((vector, mergeType), Array.fill(vector.dimension)(defaultValue))
      }
      mergeCache((vector, mergeType))
    }
  }


  def flushOne(vector: PSVector, mergeType: MergeType) = {
    mergeCache.synchronized {
      if (mergeCache.contains((vector, mergeType))) {
        val mergeArray = mergeCache((vector, mergeType))
        mergeType match {
          case INCREMENT => vectorOps.increment(vector, mergeArray)
          case MAX => vectorOps.mergeMax(vector, mergeArray)
          case MIN => vectorOps.mergeMin(vector, mergeArray)
        }
        mergeCache.remove((vector, mergeType))
      }
    }
  }

  private[spark] def flushAll() = {
    for (key <- mergeCache.keys) {
      flushOne(key._1, key._2)
    }
  }
}

object MergeType extends Enumeration {
  type MergeType = Value
  val INCREMENT, MAX, MIN = Value
}



