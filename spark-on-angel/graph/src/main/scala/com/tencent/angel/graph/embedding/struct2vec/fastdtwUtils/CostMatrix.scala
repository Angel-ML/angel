package struct2vec.fastdtwUtils

import scala.annotation.tailrec

/**
 * An entry in a matrix
 *
 * @param index     tuple representing index (i,j)
 * @param value     value of the matrix
 */
final case class MatrixEntry(index: (Int, Int), value: Double)

/**
 * A direction of travel when traversing a matrix
 */
trait Direction
object Direction extends Direction {
  case object Southwest extends Direction
  case object South extends Direction
  case object West extends Direction
}

/**
 * A matrix representing the incremental distance cost incurred by comparing two time series at indices i and j
 *
 * @param structure
 */
final case class CostMatrix(structure: Seq[MatrixEntry]*) extends Seq[Seq[MatrixEntry]] {

  def apply(i: Int): Seq[MatrixEntry] = {
    structure(i)
  }

  def apply(i: Int, j: Int): MatrixEntry = {
    structure(i)(j)
  }

  override def length: Int = structure.length

  /**
   * Prunes the top-right-most element from the cost matrix and
   *
   * @param direction
   * @return
   */
  def reduce(direction: Direction): CostMatrix = {
    val topRightY: Int = structure.lastOption.flatMap(_.lastOption.map(_.index._2)).getOrElse(0)
    import Direction._
    direction match {
      case Southwest => //prune on diagonal (column above and row to right)
        CostMatrix(structure.init.map(_.takeWhile(_.index._2 < topRightY)): _*)
      case South => CostMatrix(structure.map(_.takeWhile(_.index._2 < topRightY)): _*) //prune the row above
      case West => CostMatrix(structure.dropRight(1): _*) //prune the column to the right
    }
  }

  /**
   * `optimalPath` is the cheapest sequence of comparison indices, ie. the calculated warp path of the DTW algorithm
   * `optimalCost` is the cost of traversing this path
   */
  lazy val (optimalPath: Seq[(Int, Int)], optimalCost: Double) = {
    val initialEntries = structure.lastOption.flatMap(_.lastOption.map(o => Seq(o))).getOrElse(Seq())
    val entries: Seq[MatrixEntry] = computeOptimalPath.reverse ++ initialEntries

    entries.map(_.index) -> entries.lastOption.map(_.value).getOrElse(0D)
  }

  @tailrec
  private def dropRightWhile(f: MatrixEntry => Boolean, maxTimes: Int = Int.MaxValue)(column: Seq[MatrixEntry]): Seq[MatrixEntry] = {
    if (maxTimes == 0 || column.isEmpty || !f(column.last))
      column
    else
      dropRightWhile(f, maxTimes - 1)(column.dropRight(1))
  }
  private def shouldDrop(y: Int): MatrixEntry => Boolean = { e: MatrixEntry => e.index._2 >= y }

  private def computeOptimalPath: Seq[MatrixEntry] = {
    val lastColumn = structure.lastOption
    val secondToLastColumn = structure.dropRight(1).lastOption

    (lastColumn, secondToLastColumn) match {
      case (None, _) | (_, None) => //less than 2 columns exist, we're done
        Seq()
      case (Some(l), Some(sl)) =>
        val (_, curY) = l.last.index

        //item, south  ++ west
        val evaluate = Seq(
          dropRightWhile(shouldDrop(curY), 1)(sl).lastOption.map { _ -> Direction.Southwest },
          dropRightWhile(shouldDrop(curY), 1)(l).lastOption.map { _ -> Direction.South },
          sl.lastOption.map { _ -> Direction.West }).flatten
        if (evaluate.isEmpty) {
          Seq()
        } else {
          val (min, minAt) = evaluate.minBy(_._1.value)
          min +: reduce(minAt).computeOptimalPath
        }
    }
  }

  def asString: String = structure.foldLeft(Seq[String]()) { (acc, cur) =>
    if (acc.isEmpty) {
      cur.map(_.value.round.toString)
    } else {
      acc.zip(cur).map { case (existing, item) => s"$existing ${item.value.round}" }
    }
  }.reverse.mkString("\n")

  override def iterator: Iterator[Seq[MatrixEntry]] = structure.iterator
}

object CostMatrix {

  /** Creates a cost matrix from raw values */
  def fromValues(columns: Seq[Double]*): CostMatrix = CostMatrix(columns.foldLeft(Seq[Seq[MatrixEntry]]()) { (acc, cur) =>
    val columnIndex = acc.length
    acc :+ cur.foldLeft(Seq[MatrixEntry]()) { (col, item) =>
      col :+ MatrixEntry(columnIndex -> col.length, item)
    }
  }: _*)

}
