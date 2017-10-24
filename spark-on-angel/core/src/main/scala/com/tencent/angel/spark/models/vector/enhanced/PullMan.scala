package com.tencent.angel.spark.models.vector.enhanced

import scala.collection.concurrent.TrieMap

import com.tencent.angel.spark.client.PSClient
import com.tencent.angel.spark.models.vector.PSVector

object PullMan {
  def pullFromCache(vector: PSVector): Array[Double] = {
    if (!pullCache.contains(vector)) {
      val localArray = PSClient.instance().vectorOps.pull(vector)
      pullCache.put(vector, localArray)
    }

    pullCache(vector)
  }

  val pullCache = new TrieMap[PSVector, Array[Double]]
  val keyString = pullCache.keys.map(key => key.poolId + "_" + key.id).mkString(",")

}