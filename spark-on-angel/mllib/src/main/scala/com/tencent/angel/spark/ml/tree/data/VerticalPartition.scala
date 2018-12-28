package com.tencent.angel.spark.ml.tree.data

import com.tencent.angel.spark.ml.tree.gbdt.metadata.FeatureInfo
import com.tencent.angel.spark.ml.tree.sketch.HeapQuantileSketch
import com.tencent.angel.spark.ml.tree.util.Maths

object VerticalPartition {

  def getCandidateSplits(partitions: Seq[VerticalPartition],
                         numFeature: Int, numSplit: Int): Seq[(Int, Array[Float])] = {
    val featNNZ = new Array[Int](numFeature)
    partitions.foreach(_.indices.foreach(fid => featNNZ(fid) += 1))
    val sketches = featNNZ.map(nnz =>
      if (nnz > 0) new HeapQuantileSketch(nnz.toLong) else null)
    partitions.foreach(partition => {
      val partIndices = partition.indices
      val partValues = partition.values
      for (i <- partIndices.indices)
        sketches(partIndices(i)).update(partValues(i))
    })

    sketches.view.zipWithIndex.flatMap {
      case (sketch, fid) =>
        if (sketch != null && sketch.getN > 0)
          Iterator((fid, Maths.unique(sketch.getQuantiles(numSplit))))
        else
          Iterator.empty
    }
  }

  def discretize(partitions: Seq[VerticalPartition],
                 featureInfo: FeatureInfo): (Array[Float], DataSet) = {
    val numInstance = partitions.map(_.labels.length).sum
    val labels = new Array[Float](numInstance)
    val dataset = new DataSet(partitions.length, numInstance)
    var offset = 0
    partitions.sortBy(_.originPartId)
      .foreach(partition => {
        val partSize = partition.labels.length
        Array.copy(partition.labels, 0, labels, offset, partSize)
        val partIndexEnd = partition.indexEnd
        val partIndices = partition.indices
        val partValues = partition.values
        val partBins = new Array[Int](partIndices.length)
        for (i <- partIndices.indices) {
          partBins(i) = Maths.indexOf(featureInfo.getSplits(
            partIndices(i)), partValues(i))
        }
        dataset.setPartition(partition.originPartId, partIndices, partBins, partIndexEnd)
        offset += partSize
        println(s"OriPart[${partition.originPartId}] has $partSize instances, $numInstance in total")
      })
    require(offset == numInstance)
    (labels, dataset)
  }
}

case class VerticalPartition(originPartId: Int, labels: Array[Float], indexEnd: Array[Int],
                             indices: Array[Int], values: Array[Float])
