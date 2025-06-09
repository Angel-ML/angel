package struct2vec

import scala.collection.mutable.ArrayBuffer
import scala.util.Random

object Alias_table {

  def calcAliasTable(partId: Int, iter: Iterator[(Long, Array[(Long, Float)])]): Iterator[(Long, Array[Long], Array[Float], Array[Int])] = {
    iter.map { case (src, neighbors) =>
      val (events, weights) = neighbors.unzip
      val weightsSum = weights.sum
      val len = weights.length
      val areaRatio = weights.map(_ / weightsSum * len)
      val (accept, alias) = createAliasTable(areaRatio)
      (src, events, accept, alias)
    }
  }

  def createAliasTable(areaRatio: Array[Float]): (Array[Float], Array[Int]) = {
    val len = areaRatio.length
    val small = ArrayBuffer[Int]()
    val large = ArrayBuffer[Int]()
    val accept = Array.fill(len)(0f)
    val alias = Array.fill(len)(0)

    for (idx <- areaRatio.indices) {
      if (areaRatio(idx) < 1.0) small.append(idx) else large.append(idx)
    }
    while (small.nonEmpty && large.nonEmpty) {
      val smallIndex = small.remove(small.size - 1)
      val largeIndex = large.remove(large.size - 1)
      accept(smallIndex) = areaRatio(smallIndex)
      alias(smallIndex) = largeIndex
      areaRatio(largeIndex) = areaRatio(largeIndex) - (1 - areaRatio(smallIndex))
      if (areaRatio(largeIndex) < 1.0) small.append(largeIndex) else large.append(largeIndex)
    }
    while (small.nonEmpty) {
      val smallIndex = small.remove(small.size - 1)
      accept(smallIndex) = 1
    }

    while (large.nonEmpty) {
      val largeIndex = large.remove(large.size - 1)
      accept(largeIndex) = 1
    }
    (accept, alias)
  }

  def alias_sample(rand:Random, accept: Array[Float], alias: Array[Int], sampleNum: Int): Array[Int] = {

    val indices = new Array[Int](sampleNum)

    for (i <- (0 until sampleNum)) {
      val id = rand.nextInt(accept.length)
      val v = rand.nextDouble().toFloat
      if (v < accept(id)) {
        indices(i) = id
      } else {
        indices(i) = alias(id)
      }
    }
    indices
  }
}
