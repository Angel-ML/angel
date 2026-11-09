package com.tencent.angel.spark.ml.ann.index

import com.tencent.angel.spark.ml.ann._

trait IndexGraph extends Index with Serializable {
  
  val defaultProgressUpdateInterval: Int = 5000
  
  def add(item: Item): Boolean
  
  def addAll(items: Array[Item],
             listener: ProgressListener,
             progressUpdateInterval: Int = defaultProgressUpdateInterval): Unit = {
    
    val len = items.length
    for (idx <- items.indices) {
      this.add(items(idx))
      if (idx % progressUpdateInterval == 0 || idx == len - 1) {
        listener.updateProgress(idx + 1, len)
      }
    }
  }
  
  def size: Int
  
  def apply(id: Long): Item
  
  def get(id: Long): Option[Item]
  
  def contains(id: Long): Boolean
  
  def findNearest(vector: Array[Float], k: Int): Seq[SearchResult]
  
  def findNeighbors(id: Long, k: Int): Seq[SearchResult]
  
  def save(path: String): Boolean
  
  def load(path: String): Boolean
  
}
