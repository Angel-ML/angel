package com.tencent.angel.spark.ml.featureEngineering.Information

/**
 * Created by allylu on 2016/11/1.
 */
class InfoGainRatio extends Serializable {

  //待计算特征
  private var oneFeature = new Feature
  //目标分布的信息熵
  private var entropy: Double = 0
  //目标总样本数
  private var ActualTotal: Double = 0

  //计算分裂前的熵
  def this(oneFea: Feature, objDis: (Int, Int, Int)) = {

    this
    this.oneFeature = oneFea
    //目标标签列分布
    val labelPos = objDis._1.toDouble
    val labelNeg = objDis._2.toDouble
    val labelTotal = objDis._3.toDouble
    this.ActualTotal = labelTotal
    if (labelPos > 0) {
      this.entropy -= (labelPos / ActualTotal) * Math.log(labelPos / ActualTotal)
    }
    if (labelNeg > 0) {
      this.entropy -= (labelNeg / ActualTotal) * Math.log(labelNeg / ActualTotal)
    }
    this.entropy
  }

  //计算信息增益率
  def Compute(): Double = {

    if (entropy == 0) return 0

    var p1: Double = 0
    var p2: Double = 0
    //该特征的分裂信息度量
    var infoFeature: Double = 0
    //信息增益
    var sum: Double = 0
    var positive: Double = 0
    var negative: Double = 0
    var total: Double = 0

    for (iter <- oneFeature.values) {
      positive = iter.pos
      negative = iter.neg
      total = iter.posneg
      if (positive > 0) p1 = (positive / total) * Math.log(positive / total)
      else p1 = 0
      if (negative > 0) p2 = (negative / total) * Math.log(negative / total)
      else p2 = 0
      sum -= (total / ActualTotal) * (p1 + p2)

      infoFeature -= (total / ActualTotal) * Math.log(total / ActualTotal)
    }
    //信息增益率
    if (infoFeature > 0) (this.entropy - sum) / infoFeature else 0.0
  }

}
