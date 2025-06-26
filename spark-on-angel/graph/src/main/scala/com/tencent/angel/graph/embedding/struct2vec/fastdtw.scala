package struct2vec.fastdtw

import struct2vec.DTW
import struct2vec.fastdtwUtils.{CostMatrix, RangeDiagonalConstraints, Space, TimeSeriesElement}

/**
 * @param searchRadius
 * @param space
 * @tparam T
 */
class fastdtw[T](searchRadius: Int, space: Space[T]) {

  lazy val requiredSearchRaadius: Int = math.max(0, searchRadius)
  lazy val minTsSize: Int = requiredSearchRaadius + 2
  lazy val dtw: DTW[T] = new DTW(space)

  /**
   * @param left
   * @param right
   * @return
   */
  //@tailrec
  final def evaluate(left: Seq[TimeSeriesElement[T]], right: Seq[TimeSeriesElement[T]]): CostMatrix =
    (space.coarsen(left.length / 2), space.coarsen(right.length / 2), left.size > minTsSize && right.size > minTsSize) match {
      case (Some(coarsenLeft), Some(coarsenRight), true) =>
        val newLeft = coarsenLeft(left)
        val newRight = coarsenRight(right)
        dtw.costMatrix(left, right, RangeDiagonalConstraints.fromCostMatrix(evaluate(newLeft, newRight)))
      case _ => dtw.costMatrix(left, right)
    }
}
