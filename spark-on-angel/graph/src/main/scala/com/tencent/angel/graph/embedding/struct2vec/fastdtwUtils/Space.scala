package struct2vec.fastdtwUtils

/**
 * A type, a metric, and some associated functions that allow coarsening and estimation of missing value contributions.
 *
 * @tparam T
 */
trait Space[T] {
  /**
   * The value to add to a distance metric when a value is missing
   *
   * @param otherValue
   * @return
   */
  def missingValue(otherValue: Option[T]): Double

  /**
   * The distance function between two `T` when the values exist
   *
   * @return
   */
  def metric: (T, T) => Double

  /**
   * Takes non-overlapping groups of `resolution` points and applies a coarsening operation to them such that
   * the value on the neighborhood so defined is minimized when it contains a point that's a
   * minimum under the existing metric at high resolution.
   *
   * Should be `None` if no such functions are known.
   *
   * @param resolution
   * @return
   */
  def coarsen(resolution: Int): Option[Seq[TimeSeriesElement[T]] => Seq[TimeSeriesElement[T]]]

  /**
   * The distance function accounting for missing values
   *
   * @return
   */
  def distance: (Option[T], Option[T]) => Double = {
    case (Some(a), Some(b)) => metric(a, b)
    case (None, a) => missingValue(a)
    case (a, None) => missingValue(a)
  }

}

/**
 * Mixin for spaces where a missing value is ignored in the distance calculations
 *
 * @tparam T
 */
trait MissingValueContributesNothing[T] { space: Space[T] =>
  def missingValue(otherValue: Option[T]): Double = 0
}

/**
 * Vector[Double] space with a standard euclidean metric and ignored missing values
 */
object EuclideanSpace extends Space[VectorValue[Double]] with MissingValueContributesNothing[VectorValue[Double]] {
  override def metric: (VectorValue[Double], VectorValue[Double]) => Double = {
    (l: VectorValue[Double], r: VectorValue[Double]) =>
      val ds = r.v.zip(l.v).foldLeft(0D) { (acc, cur) =>
        val d = cur._1 - cur._2
        acc + d * d
      }
      math.sqrt(ds)
  }

  override def coarsen(resolution: Int): Option[Seq[TimeSeriesElement[VectorValue[Double]]] => Seq[TimeSeriesElement[VectorValue[Double]]]] = Some { input =>
    input.grouped(input.size / resolution).map { groupedItems: Seq[TimeSeriesElement[VectorValue[Double]]] => //grouped items need to all be added together
      val nonEmptyItems = groupedItems.dropWhile(_.v.isEmpty)
      val summedVector = VectorValue(nonEmptyItems.drop(1).foldLeft(nonEmptyItems.head.v.get.toSeq) { (acc: Seq[Double], cur: TimeSeriesElement[VectorValue[Double]]) =>
        cur.v.map { vv =>
          acc.zip(vv).map { case (l: Double, r: Double) => l + r }
        }.getOrElse(acc) //assumption about missing values here: they have no effect
      }.map(_ / groupedItems.length.toDouble): _*)
      TimeSeriesElement(groupedItems.head.t, Some(summedVector))
    }.toSeq
  }
}

/**
 * String space counting locations where strings don't match and ignoring missing contributions
 */
object HammingSpace extends Space[String] with MissingValueContributesNothing[String] {
  override def metric: (String, String) => Double = { (i, j) => i.zip(j).count { case (l, r) => l != r } }

  override def coarsen(resolution: Int): Option[Seq[TimeSeriesElement[String]] => Seq[TimeSeriesElement[String]]] = Some { input =>
    input.grouped(input.size / resolution).map { groupedItems =>
      val v: String = groupedItems.foldLeft("") { (acc: String, cur: TimeSeriesElement[String]) =>
        val s: String = cur.v.getOrElse("")
        acc + s
      }
      TimeSeriesElement(groupedItems.head.t, Some(v))
    }.toSeq
  }
}

/**
 * String space using a Jaccard metric and ignoring missing contributions
 */
object JaccardSpace extends Space[String] with MissingValueContributesNothing[String] {
  override def metric: (String, String) => Double = {
    case (i: String, j: String) if i.nonEmpty || j.nonEmpty =>
      val is = i.toSet
      val js = j.toSet
      val intersect = (is intersect js).size.toDouble
      intersect / (is.size.toDouble + js.size.toDouble - intersect)
    case _ => 1
  }

  override def coarsen(resolution: Int): Option[Seq[TimeSeriesElement[String]] => Seq[TimeSeriesElement[String]]] = None
}
