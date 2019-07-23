package com.tencent.angel.ml.tree.oldmodel

import com.tencent.angel.ml.tree.conf.FeatureType.FeatureType

/**
  * Split applied to a feature
  * @param feature feature index
  * @param threshold Threshold for continuous feature.
  *                  Split left if feature is less than or equal to threshold, else right.
  * @param featureType type of feature -- categorical or continuous
  * @param categories Split left if categorical feature value is in this set, else right.
  */
case class Split(
                  feature: Int,
                  threshold: Float,
                  featureType: FeatureType,
                  categories: List[Float]) {

  override def toString: String = {
    s"Feature = $feature, threshold = $threshold, featureType = $featureType, " +
      s"categories = $categories"
  }
}

/**
  * Split with minimum threshold for continuous features. Helps with the smallest bin creation.
  * @param feature feature index
  * @param featureType type of feature -- categorical or continuous
  */
private[tree] class DummyLowSplit(feature: Int, featureType: FeatureType)
  extends Split(feature, Float.MinValue, featureType, List())

/**
  * Split with maximum threshold for continuous features. Helps with the highest bin creation.
  * @param feature feature index
  * @param featureType type of feature -- categorical or continuous
  */
private[tree] class DummyHighSplit(feature: Int, featureType: FeatureType)
  extends Split(feature, Float.MaxValue, featureType, List())

/**
  * Split with no acceptable feature values for categorical features. Helps with the first bin
  * creation.
  * @param feature feature index
  * @param featureType type of feature -- categorical or continuous
  */
private[tree] class DummyCategoricalSplit(feature: Int, featureType: FeatureType)
  extends Split(feature, Float.MaxValue, featureType, List())

