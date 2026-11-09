package com.tencent.angel.spark.ml.ann.partition

import com.tencent.angel.spark.ml.ann.index.HnswIndex.HnswIndexBuilder
import com.tencent.angel.spark.ml.ann.index.{HnswIndex, IndexGraph}
import com.tencent.angel.spark.ml.ann.{Item, ProgressListener}

class IVFHnswPartition(sheetId: Int,
                       partIndex: Int,
                       graphKeys: Array[Long],
                       graphIndexModel: IndexGraph,
                       val vectors: Array[Array[Float]])
  extends IVFHnswGraphPartition(sheetId, partIndex, graphKeys, graphIndexModel) with Serializable {
  
  def buildGraph(): IVFHnswPartition = {
    println(s"start building graph part")
    val timePartBuildGraph = System.currentTimeMillis()
    val items = graphKeys.zip(vectors).map{ it =>
      new Item(it._1, it._2)
    }
    graphIndexModel.addAll(items, new ProgressListener((done, max) =>
      println(s"$done of $max vectors have been added")))
    println(s"part $partIndex build graph index cost: ${(System.currentTimeMillis() - timePartBuildGraph) / 1000.0} s")

    new IVFHnswPartition(sheetId, partIndex, graphKeys, graphIndexModel, vectors)
  }
  
  override def setSheetId(id: Int): IVFHnswPartition = {
    new IVFHnswPartition(id, partIndex, graphKeys, graphIndexModel, vectors)
  }
  
}



object IVFHnswPartition {
  def apply(index: Int, iter: Iterator[(Long, Array[Float])], indexBuilder: HnswIndexBuilder): IVFHnswPartition = {
    val partData = iter.toArray
    val keys = partData.map(_._1)
    val vectors = partData.map(_._2)

    val hnswIndex = new HnswIndex()
    hnswIndex.hnswIndexBuilder(indexBuilder)

    new IVFHnswPartition(0, index, keys, hnswIndex, vectors)
  }

}
