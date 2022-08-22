package com.tencent.angel.graph.embedding.struct2vec

import com.tencent.angel.graph.embedding.struct2vec.fastdtwUtils.{ CostMatrix, IndexConstraints, MatrixEntry, PassthroughIndexConstraints, Space, TimeSeriesElement }

/**
 * A wrapper for the cost matrix calculation based on a specified metric
 *
 * @param space
 */
class DTW[T](space: Space[T]) {

  /**
   * @param left
   * @param right
   * @param constraints
   * @return
   */
  def costMatrix(left: Seq[TimeSeriesElement[T]], right: Seq[TimeSeriesElement[T]], constraints: IndexConstraints = new PassthroughIndexConstraints): CostMatrix =
    (left, right) match {
      case (Nil, _) | (_, Nil) => //one of the series is empty
        CostMatrix()
      case _ => CostMatrix({

        //     COST MATRIX:
        //   5|_|_|_|_|_|_|E| E = min Global Cost
        //   4|_|_|_|_|_|_|_| S = Start point
        //   3|_|_|_|_|_|_|_| each cell = min global cost to get to that point
        // j 2|_|_|_|_|_|_|_|
        //   1|_|_|_|_|_|_|_|
        //   0|S|_|_|_|_|_|_|
        //     0 1 2 3 4 5 6
        //            i
        //   access is M(i,j)... column-row

        left.foldLeft(Seq[Seq[MatrixEntry]]()) { (columns, curLeft: TimeSeriesElement[T]) =>
          val i = columns.length //this is the x-index
          val (smallestIndex, _) = constraints.columnRange(i, right.length - 1)
          val constrainedRight: Seq[TimeSeriesElement[T]] = constraints.mask(right, i)
          columns :+ (
            if (columns.isEmpty) {
              //i = 0
              //create a matrix entry for every j within the applied constraints
              constrainedRight.foldLeft(Seq[MatrixEntry]()) { (column: Seq[MatrixEntry], curLeft: TimeSeriesElement[T]) =>
                val lastDistance = column.lastOption.getOrElse(MatrixEntry(i -> 0, smallestIndex))
                column :+ MatrixEntry(i -> (smallestIndex + column.length), lastDistance.value + space.distance(left.head.v, curLeft.v))
              }
            } else {
              //i != 0
              val lastColumn: Seq[MatrixEntry] = constraints.constrain(columns.last, i) //i-1, *
              val bottomElement: MatrixEntry = MatrixEntry(i -> smallestIndex, lastColumn.head.value + space.distance(curLeft.v, constrainedRight.head.v))

              val filledOptionalLastColumn = lastColumn.map { v => Option(v) } ++ Seq.fill(constrainedRight.length)(None)
              val slidingValues: Seq[Seq[Option[MatrixEntry]]] = filledOptionalLastColumn.sliding(2).toIndexedSeq
              val neighbors: Seq[(TimeSeriesElement[T], Seq[Option[MatrixEntry]])] = constrainedRight.drop(1).zip(slidingValues)

              //neighbors is an element in the right series with the cost matrix entries for the West and Southwest directions
              neighbors.foldLeft(Seq(bottomElement)) { (column: Seq[MatrixEntry], neighborhood: (TimeSeriesElement[T], Seq[Option[MatrixEntry]])) =>

                val j = column.length + smallestIndex
                val curRight = neighborhood._1
                val neighboringValues: Seq[Option[MatrixEntry]] = neighborhood._2

                val costSouth: Double = column.last.value //i, j-1
                val minGlobalCost = (Seq(costSouth) ++ neighboringValues.flatten.map(_.value)).min

                //add an entry to the current column (for curLeft) containing the total cost to consider curRight a match
                column :+ MatrixEntry(i -> j, minGlobalCost + space.distance(curLeft.v, curRight.v))
              }

            })

        }

      }: _*)
    }

}
