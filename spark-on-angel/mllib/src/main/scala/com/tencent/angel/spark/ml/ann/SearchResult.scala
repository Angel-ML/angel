package com.tencent.angel.spark.ml.ann

import java.util.Objects

class SearchResult(private val item: Item,
                   private val distance: Float,
                   private val distanceComparator: Ordering[Float]) extends Ordered[SearchResult] with Serializable {
  
  def this(it: Item, dis: Float) = this(it, dis, Ordering.fromLessThan[Float]((a, b) => a < b))
  
  def getItem: Item = this.item
  def getDistance: Float = this.distance
  
  override def compare(that: SearchResult): Int = {
    this.distanceComparator.compare(this.distance, that.distance)
  }
  
  override def equals(obj: Any): Boolean = {
    if (obj == this) true
    else if (obj == null || obj.getClass != this.getClass) false
    else {
      val that = obj.asInstanceOf[SearchResult]
      if (Objects.equals(this.distance, that.distance) && Objects.equals(this.item, that.item)) true
      else false
    }
  }
  
  override def hashCode(): Int = {
    Objects.hash(this.distance.asInstanceOf[Object], this.item)
  }
  
  override def toString: String = {
    s"SearchResult{item: $item , distance: $distance }"
  }
  
}

object SearchResult {
  def apply(item: Item, distance: Float,
            distanceComparator: Ordering[Float]): SearchResult =
    new SearchResult(item, distance, distanceComparator)
  
  def unapply(result: SearchResult): Option[(Item, Float)] =
    Some(result.item, result.distance)
}
