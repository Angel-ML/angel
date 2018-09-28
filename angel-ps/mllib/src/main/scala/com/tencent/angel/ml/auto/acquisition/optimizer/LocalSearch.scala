package com.tencent.angel.ml.auto.acquisition.optimizer

import com.tencent.angel.ml.auto.acquisition.BaseAcquisition
import com.tencent.angel.ml.math2.vector.Vector

/**
  * Implementation of local search.
  * @param acqFunc
  */
class LocalSearch(acqFunc: BaseAcquisition) extends BaseOptimizer(acqFunc) {

  /**
    * Maximizes the given acquisition function.
    *
    * @param batchSize : Number of queried points.
    * @return A set of points with highest acquisition value.
    */
  override def maximize(batchSize: Int): Array[Vector] = ???

  override def maximize: Vector = ???
}
