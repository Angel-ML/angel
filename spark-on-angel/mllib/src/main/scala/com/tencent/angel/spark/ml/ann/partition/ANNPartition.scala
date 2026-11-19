package com.tencent.angel.spark.ml.ann.partition

import com.tencent.angel.spark.ml.ann.ANNPSModel
import org.apache.spark.broadcast.Broadcast

abstract class ANNPartition(val sheetId: Int, val partIndex: Int) extends Serializable {
  def process(psModel: ANNPSModel, queryIds: Broadcast[Array[Long]],
              topK: Int, batchSize: Int): Unit
  def findNearest(vec: Array[Float], k: Int): Array[(Long, Float)]
  def setSheetId(id: Int): ANNPartition
}
