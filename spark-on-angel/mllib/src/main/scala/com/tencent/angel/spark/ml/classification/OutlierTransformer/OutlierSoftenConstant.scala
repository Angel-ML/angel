package com.tencent.angel.spark.ml.classification.OutlierTransformer

/**
  * Created by endy on 2019/1/15.
  */
object OutlierSoftenConstant {
  // main parameter
  val method = "Method"

  // sub parameter
  val upperPercentage = "Upper Percentage"
  val lowerPercentage = "Lower Percentage"
  val upperThreshold = "Upper Threshold"
  val lowerThreshold = "Lower Threshold"

  // parameter doc
  val methodDoc: String = "Method to soften the outlier of the selected attributes." +
    "The selected attributes must be double."
  val upperPerDoc: String = "The upper bound of percentage method. " +
    "Must be in the range [0, 1] and bigger than lowerPercentage."
  val lowerPerDoc: String = "The lower bound of percentage method. " +
    "Must be in the range [0, 1] and smaller than upperPercentage."
  val upperThresholdDoc: String = "The upper bound of threshold method. " +
    "Must be numeric and bigger than lowerThreshold."
  val lowerThresholdDoc: String = "The lower bound of threshold method. " +
    "Must be numeric and smaller than upperThreshold."

  val colNames = "colNames"
  val softenOutlier = "softenOutlier"

  // supported method
  val zScore = "zScore"
  val percentage = "percentage"
  val threshold = "threshold"
  val supportedMethod = Array(zScore, percentage, threshold)

  // default value
  val defaultMethod: String = zScore
}
