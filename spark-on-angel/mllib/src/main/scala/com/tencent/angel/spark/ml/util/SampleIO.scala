package com.tencent.angel.spark.ml.util

import com.tencent.angel.ml.feature.LabeledData
import org.apache.spark.rdd.RDD

object SampleIO {
  def loadSamples(dataType: String, dim: Long, inputData: RDD[String]): RDD[LabeledData] = {
    val data = dataType match {
      case "libsvm" =>
        inputData.map(s => (DataLoader.parseLongFloat(s, dim), DataLoader.parseLabel(s, false)))

      case "dummy" =>
        inputData.map(s => (DataLoader.parseLongDummy(s, dim), DataLoader.parseLabel(s, false)))
    }

    data.map {
      f =>
        f._1.setY(f._2)
        f._1
    }.filter(f => f.getX.getSize > 0)
  }

  def loadSamples(dataType: String, dim: Long, inputData: RDD[String], isTraining: Boolean, hasLabel: Boolean): RDD[LabeledData] = {
    val data = dataType match {
      case "libsvm" =>
        inputData.map(s =>
          (DataLoader.parseLongFloat(s, dim, isTraining, hasLabel)))
      case "dummy" =>
        inputData.map(s =>
          (DataLoader.parseLongDummy(s, dim, isTraining, hasLabel)))
    }
    data
  }
}
