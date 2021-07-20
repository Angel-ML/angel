package com.tencent.angel.spark.ml.featureEngineering.Information

/**
 * Created by allylu on 2016/11/2.
 */
class GiniIndex(oneFeature: Feature, objDis: (Int, Int, Int)) extends Serializable {

  private var gini: Double = 0
  private val ActualTotal: Double = objDis._3

  def Compute(): Double = {
    var p: Double = 0
    var positive: Double = 0
    var negative: Double = 0
    var total: Double = 0

    for (iter <- oneFeature.values) {
      positive = iter.pos
      negative = iter.neg
      total = iter.posneg

      p = 2 * (positive / total) * (negative / total)
      this.gini += (total / ActualTotal) * p
    }
    //信息增益率
    this.gini
  }

}
