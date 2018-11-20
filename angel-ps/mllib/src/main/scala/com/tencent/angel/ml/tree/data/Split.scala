package com.tencent.angel.ml.tree.data

import java.util.Objects

import com.tencent.angel.ml.math2.vector.IntFloatVector
import com.tencent.angel.ml.tree.conf.{FeatureType => OldFeatureType}
import com.tencent.angel.ml.tree.oldmodel.{Split => OldSplit}


/**
  * Interface for a "Split," which specifies a test made at a decision tree node
  * to choose the left or right path.
  */
sealed trait Split extends Serializable {

  /** Index of feature which this split tests */
  def featureIndex: Int

  /**
    * Return true (split to left) or false (split to right).
    * @param features  Vector of features (original values, not binned).
    */
  private[tree] def shouldGoLeft(features: IntFloatVector): Boolean

  /**
    * Return true (split to left) or false (split to right).
    * @param binnedFeature Binned feature value.
    * @param splits All splits for the given feature.
    */
  private[tree] def shouldGoLeft(binnedFeature: Int, splits: Array[Split]): Boolean

  /** Convert to old Split format */
  private[tree] def toOld: OldSplit
}

private[tree] object Split {

  def fromOld(oldSplit: OldSplit, categoricalFeatures: Map[Int, Int]): Split = {
    oldSplit.featureType match {
      case OldFeatureType.Categorical =>
        new CategoricalSplit(featureIndex = oldSplit.feature,
          _leftCategories = oldSplit.categories.toArray, categoricalFeatures(oldSplit.feature))
      case OldFeatureType.Continuous =>
        new ContinuousSplit(featureIndex = oldSplit.feature, threshold = oldSplit.threshold)
    }
  }
}

/**
  * Split which tests a categorical feature.
  * @param featureIndex  Index of the feature to test
  * @param _leftCategories  If the feature value is in this set of categories, then the split goes
  *                         left. Otherwise, it goes right.
  * @param numCategories  Number of categories for this feature.
  */
class CategoricalSplit private[tree] (
                                     override val featureIndex: Int,
                                     _leftCategories: Array[Float],
                                     val numCategories: Int)
  extends Split {

  require(_leftCategories.forall(cat => 0 <= cat && cat < numCategories), "Invalid leftCategories" +
    s" (should be in range [0, $numCategories)): ${_leftCategories.mkString(",")}")

  /**
    * If true, then "categories" is the set of categories for splitting to the left, and vice versa.
    */
  private val isLeft: Boolean = _leftCategories.length <= numCategories / 2

  /** Set of categories determining the splitting rule, along with [[isLeft]]. */
  private val categories: Set[Float] = {
    if (isLeft) {
      _leftCategories.toSet
    } else {
      setComplement(_leftCategories.toSet)
    }
  }

  override private[ml] def shouldGoLeft(features: IntFloatVector): Boolean = {
    if (isLeft) {
      categories.contains(features.get(featureIndex))
    } else {
      !categories.contains(features.get(featureIndex))
    }
  }

  override private[tree] def shouldGoLeft(binnedFeature: Int, splits: Array[Split]): Boolean = {
    if (isLeft) {
      categories.contains(binnedFeature.toFloat)
    } else {
      !categories.contains(binnedFeature.toFloat)
    }
  }

  override def hashCode(): Int = {
    val state = Seq(featureIndex, isLeft, categories)
    state.map(Objects.hashCode).foldLeft(0)((a, b) => 31 * a + b)
  }

  override def equals(o: Any): Boolean = o match {
    case other: CategoricalSplit => featureIndex == other.featureIndex &&
      isLeft == other.isLeft && categories == other.categories
    case _ => false
  }

  override private[tree] def toOld: OldSplit = {
    val oldCats = if (isLeft) {
      categories
    } else {
      setComplement(categories)
    }
    OldSplit(featureIndex, 0.0f, OldFeatureType.Categorical, oldCats.toList)
  }

  /** Get sorted categories which split to the left */
  def leftCategories: Array[Float] = {
    val cats = if (isLeft) categories else setComplement(categories)
    cats.toArray.sorted
  }

  /** Get sorted categories which split to the right */
  def rightCategories: Array[Float] = {
    val cats = if (isLeft) setComplement(categories) else categories
    cats.toArray.sorted
  }

  /** [0, numCategories) \ cats */
  private def setComplement(cats: Set[Float]): Set[Float] = {
    Range(0, numCategories).map(_.toFloat).filter(cat => !cats.contains(cat)).toSet
  }
}

/**
  * Split which tests a continuous feature.
  * @param featureIndex  Index of the feature to test
  * @param threshold  If the feature value is less than or equal to this threshold, then the
  *                   split goes left. Otherwise, it goes right.
  */
class ContinuousSplit private[tree] (override val featureIndex: Int, val threshold: Float)
  extends Split {

  override private[tree] def shouldGoLeft(features: IntFloatVector): Boolean = {
    features.get(featureIndex) <= threshold
  }

  override private[tree] def shouldGoLeft(binnedFeature: Int, splits: Array[Split]): Boolean = {
    if (binnedFeature == splits.length) {
      // > last split, so split right
      false
    } else {
      val featureValueUpperBound = splits(binnedFeature).asInstanceOf[ContinuousSplit].threshold
      featureValueUpperBound <= threshold
    }
  }

  override def equals(o: Any): Boolean = {
    o match {
      case other: ContinuousSplit =>
        featureIndex == other.featureIndex && threshold == other.threshold
      case _ =>
        false
    }
  }

  override def hashCode(): Int = {
    val state = Seq(featureIndex, threshold)
    state.map(Objects.hashCode).foldLeft(0)((a, b) => 31 * a + b)
  }

  override private[tree] def toOld: OldSplit = {
    OldSplit(featureIndex, threshold, OldFeatureType.Continuous, List.empty[Float])
  }
}
