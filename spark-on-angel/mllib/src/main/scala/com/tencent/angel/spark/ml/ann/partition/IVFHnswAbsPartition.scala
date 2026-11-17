package com.tencent.angel.spark.ml.ann.partition

import com.tencent.angel.spark.ml.ann.{ANNPSModel, IVFHnswPSModel}
import org.apache.spark.broadcast.Broadcast

abstract class IVFHnswAbsPartition(val sheetId: Int, val partIndex: Int) extends Serializable {
  def process(psModel: IVFHnswPSModel, queryIds: Broadcast[Array[Long]],
              topK: Int, batchSize: Int): Unit

  def findNearest(vec: (Array[Long], Array[Array[Float]]), k: Int): Array[(Long,Array[(Long, Float)])]
  def setSheetId(id: Int): IVFHnswAbsPartition
}
