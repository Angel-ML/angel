package com.tencent.angel.spark.ml.tree.data

import org.apache.spark.ml.linalg.Vector
import com.tencent.angel.spark.ml.tree.exception.GBDTException

object Instance {
  def ensureLabel(labels: Array[Float], numLabel: Int): Unit = {
    if (numLabel == 2) {
      val distinct = labels.distinct
      if (distinct.length < 2)
        throw new GBDTException("All labels equal to " + distinct.head.toInt)
      else if (distinct.length > 2)
        throw new GBDTException("More than 2 labels are provided: " + distinct.mkString(", "))
      else if (distinct.contains(-1f)) {
        println("[WARN] Change labels from -1 to 0")
        for (i <- labels.indices)
          if (labels(i) == -1f)
            labels(i) = 0f
      }
    } else {
      var min = Integer.MAX_VALUE
      var max = Integer.MIN_VALUE
      for (label <- labels) {
        val trueLabel = label.toInt
        min = Math.min(min, trueLabel)
        max = Math.max(max, trueLabel)
        if (label < 0 || label > numLabel)
          throw new GBDTException("Incorrect label: " + trueLabel)
      }
      if (max - min >= numLabel) {
        throw new GBDTException(s"Invalid range for labels: [$min, $max]")
      } else if (max == numLabel) {
        println(s"[WARN] Change range of labels from [1, $numLabel] to [0, ${numLabel - 1}]")
        for (i <- labels.indices)
          labels(i) -= 1f
      }
    }
  }
}

case class Instance(label: Double, feature: Vector) {
  override def toString: String = {
    s"($label, $feature)"
  }
}
