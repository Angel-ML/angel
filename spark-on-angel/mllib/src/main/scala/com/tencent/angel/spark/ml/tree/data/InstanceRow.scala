package com.tencent.angel.spark.ml.tree.data

import java.{util => ju}
import com.tencent.angel.spark.ml.tree.gbdt.histogram.Histogram
import com.tencent.angel.spark.ml.tree.gbdt.histogram.GradPair

//case class InstanceRow(indices: Array[Int], bins: Array[Int]) {
//
//  def size: Int = if (indices == null) 0 else indices.length
//}

object InstanceRow {
  def apply(indices: Array[Int], bins: Array[Int]): InstanceRow =
    new InstanceRow(indices, bins, 0, indices.length)

  def apply(allIndices: Array[Int], allBins: Array[Int],
            from: Int, until: Int): InstanceRow = new InstanceRow(allIndices, allBins, from, until)
}

class InstanceRow(private var allIndices: Array[Int], private var allBins: Array[Int],
                  private var start: Int, private var end: Int) extends Serializable {

  def get(fid: Int): Int = {
    val t = ju.Arrays.binarySearch(allIndices, start, end, fid)
    if (t >= 0) allBins(t) else -1
  }

  def accumulate(histograms: Array[Histogram], gradPair: GradPair,
                 isFeatUsed: Array[Boolean], featOffset: Int = 0): Unit = {
    for (i <- start until end) {
      if (isFeatUsed(allIndices(i) - featOffset))
        histograms(allIndices(i) - featOffset).accumulate(allBins(i), gradPair)
    }
  }

  def indices: Array[Int] = ???

  def bins: Array[Int] = ???

  def size: Int = end - start
}
