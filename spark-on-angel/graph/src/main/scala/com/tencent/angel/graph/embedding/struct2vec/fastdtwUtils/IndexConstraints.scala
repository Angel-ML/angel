package struct2vec.fastdtwUtils

/**
 * A contract for constraining which indices should be explored in a DTW traversal
 */
trait IndexConstraints {
  /** Returns the min and max row indices for a given column index with a maximum number of entries */
  def columnRange(i: Int, max: Int): (Int, Int)

  /** Applies `columnRange` to a column of `TimeAndVector` entries */
  def mask[T](t: Seq[TimeSeriesElement[T]], column: Int): Seq[TimeSeriesElement[T]]

  /** Applies `columnRange` to a column of `MatrixEntry`s */
  def constrain(t: Seq[MatrixEntry], column: Int): Seq[MatrixEntry]
}

/**
 * A base class for constraints that are constrained around the diagonal of the matrix
 */
abstract class DiagonalConstraints() extends IndexConstraints {
  def mask[T](t: Seq[TimeSeriesElement[T]], column: Int): Seq[TimeSeriesElement[T]] = {
    val (minIndex, maxIndex) = columnRange(column, t.length - 1)
    t.slice(minIndex, maxIndex + 1)
  }

  def constrain(t: Seq[MatrixEntry], column: Int): Seq[MatrixEntry] = {
    val (minIndex, maxIndex) = columnRange(column, t.maxBy(_.index._2).index._2)
    t.filter { e => e.index._2 >= minIndex && e.index._2 <= maxIndex }
  }
}

/**
 * Constraints allowing exploration of a window above and below the diagonal
 *
 * eg. applying window = 1 to
 *  1 2 3 4        x x 3 4
 *  5 6 7 8    =>  x 6 7 8
 *  9 8 7 6        9 8 7 x
 *  5 4 3 2        5 4 x x
 *
 * @param window radius of the window around the diagonal
 */
final case class UniformDiagonalConstraints(window: Int = 3) extends DiagonalConstraints {
  def columnRange(i: Int, max: Int): (Int, Int) = math.max(0, i - window) -> math.min(max, i + window)
}

/**
 * Constraints allowing arbitrary specification of constraints by column. Defaults to all values if a
 * column's range is not explicitly specified
 *
 * @param ranges  explicit map of column index => range
 */
final case class RangeDiagonalConstraints(ranges: Map[Int, (Int, Int)]) extends DiagonalConstraints {
  def columnRange(i: Int, max: Int): (Int, Int) = ranges.get(i).map {
    case (lower, upper) =>
      lower -> math.min(upper, max)
  }.getOrElse(0 -> max)
}

object RangeDiagonalConstraints {

  /**
   * Calculates the expected `RangeDiagonalConstraints` from a `CostMatrix`'s optimal path.
   *  Assumes the cost matrix is sorted by its indices and has values specified
   *
   * @param lowResCost
   * @return
   */
  def fromCostMatrix(lowResCost: CostMatrix): RangeDiagonalConstraints = RangeDiagonalConstraints {
    lowResCost.optimalPath.foldLeft(Map[Int, (Int, Int)]()) {
      case (acc, cur) =>
        //assume matrix columns are in order and are populated
        val minY = 2 * cur._2
        val maxY = 2 * cur._2 + 1

        val currentIndex = 2 * cur._1
        val previousIndex = currentIndex - 1
        val currentRange = acc.get(currentIndex)
        val currentRangeNext = acc.get(currentIndex + 1)
        val previousRange = acc.get(previousIndex)
        acc ++ {
          (currentRange, currentRangeNext, previousRange) match {
            case (Some((mn, mx)), Some((mnn, mxn)), _) =>
              val range1 = math.min(mn, minY) -> math.max(mx, maxY)
              val range2 = math.min(mnn, minY) -> math.max(mxn, maxY)
              Map(currentIndex -> range1, currentIndex + 1 -> range2)
            case (_, _, Some((mn, mx))) if mx < minY =>
              Map(currentIndex -> (minY - 1 -> maxY), currentIndex + 1 -> (minY -> maxY), previousIndex -> (mn -> (mx + 1)))
            case _ =>
              Map(currentIndex -> (minY -> maxY), currentIndex + 1 -> (minY -> maxY))
          }
        }
    }
  }
}

/**
 * A fully permissive `IndexConstraints` implementation
 */
class PassthroughIndexConstraints extends IndexConstraints {
  def columnRange(i: Int, max: Int): (Int, Int) = 0 -> max
  def mask[T](t: Seq[TimeSeriesElement[T]], column: Int): Seq[TimeSeriesElement[T]] = t
  def constrain(t: Seq[MatrixEntry], column: Int): Seq[MatrixEntry] = t
}