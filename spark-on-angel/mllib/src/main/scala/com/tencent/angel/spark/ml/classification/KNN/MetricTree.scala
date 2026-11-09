package org.apache.spark.ml.classification.knn


import breeze.linalg.{max, min, squaredDistance}
import org.apache.spark.ml.classification.knn.KNN.{RowWithVector, VectorWithNorm}
import org.apache.spark.ml.linalg.{Vector, Vectors}
import org.apache.spark.util.random.XORShiftRandom

import scala.collection.mutable


abstract class Tree extends Serializable {
  val leftChild: Tree
  val rightChild: Tree
  val size: Int
  val leafCount: Int
  val pivot: VectorWithNorm
  val radius: Double

  def iterator: Iterator[RowWithVector]

  /**
   * k-NN query using pre-built [[Tree]]
   *
   * @param v vector to query
   * @param k number of nearest neighbor
   * @return a list of neighbor that is nearest to the query vector
   */
  def query(v: Vector, k: Int = 1): Iterable[(RowWithVector, Double)] = {
    query(new VectorWithNorm(v), k)
  }

  def query(v: VectorWithNorm, k: Int): Iterable[(RowWithVector, Double)] = {
    query(new KNNCandidates(v, k)).toIterable
  }

  /**
   * Refine k-NN candidates using data in this [[Tree]]
   */
  private[knn] def query(candidates: KNNCandidates): KNNCandidates

  /**
   * Compute QueryCost defined as || v.center - q || - r
   * when >= v.r node can be pruned
   * for MetricNode this can be used to determine which child does queryVector falls into
   */
  private[knn] def distance(candidates: KNNCandidates): Double = distance(candidates.queryVector)

  private[knn] def distance(v: VectorWithNorm): Double =
    if (pivot.vector.size > 0) pivot.fastDistance(v) else 0.0
}

case object Empty extends Tree {
  override val leftChild = this
  override val rightChild = this
  override val size = 0
  override val leafCount = 0
  override val pivot = new VectorWithNorm(Vectors.dense(Array.empty[Double]))
  override val radius = 0.0

  override def iterator: Iterator[RowWithVector] = Iterator.empty

  override def query(candidates: KNNCandidates): KNNCandidates = candidates
}

case class Leaf(data: IndexedSeq[RowWithVector],
                pivot: VectorWithNorm,
                radius: Double) extends Tree {
  override val leftChild = Empty
  override val rightChild = Empty
  override val size = data.size
  override val leafCount = 1

  override def iterator: Iterator[RowWithVector] = data.iterator

  // brute force k-NN search at the leaf
  override def query(candidates: KNNCandidates): KNNCandidates = {
    val sorted = data
      .map { v => (v, candidates.queryVector.fastDistance(v.vector)) }
      .sortBy(_._2)

    for ((v, d) <- sorted if candidates.notFull || d < candidates.maxDistance)
      candidates.insert(v, d)

    candidates
  }
}

object Leaf {
  def apply(data: IndexedSeq[RowWithVector]): Leaf = {
    val vectors = data.map(_.vector.vector.asBreeze)
    val (minV, maxV) = vectors.foldLeft((vectors.head, vectors.head)) {
      case ((accMin, accMax), bv) =>
        (min(accMin, bv), max(accMax, bv))
    }
    val pivot = new VectorWithNorm((minV + maxV) / 2.0)
    val radius = math.sqrt(squaredDistance(minV, maxV)) / 2.0
    Leaf(data, pivot, radius)
  }
}

/**
 * A [[MetricTree]] represents a MetricNode where data are split into two partitions: left
 * and right.
 * There exists two pivot vectors: leftPivot and rightPivot to determine the partitioning.
 * Pivot vector should be the middle of leftPivot and rightPivot vectors.
 * Points that is closer to leftPivot than to rightPivot belongs to leftChild and rightChild
 * otherwise.
 *
 * During search, because we have information about each child's pivot and radius, we can see
 * if the
 * hyper-sphere intersects with current candidates sphere. If so, we search the child that has the
 * most potential (i.e. the child which has the closest pivot).
 * Once that child has been fully searched, we backtrack to the remaining child and search
 * if necessary.
 *
 * This is much more efficient than naive brute force search. However backtracking can take a
 * lot of time when the number of dimension is high (due to longer time to compute distance and
 * the volume growing much faster than radius).
 */

case class MetricTree(leftChild: Tree,
                      leftPivot: VectorWithNorm,
                      rightChild: Tree,
                      rightPivot: VectorWithNorm,
                      pivot: VectorWithNorm,
                      radius: Double
                     ) extends Tree {
  override val size = leftChild.size + rightChild.size
  override val leafCount = leftChild.leafCount + rightChild.leafCount

  override def iterator: Iterator[RowWithVector] = leftChild.iterator ++ rightChild.iterator

  override def query(candidates: KNNCandidates): KNNCandidates = {
    lazy val leftQueryCost = leftChild.distance(candidates)
    lazy val rightQueryCost = rightChild.distance(candidates)
    // only query if at least one of the children is worth looking
    if (candidates.notFull ||
      leftQueryCost - candidates.maxDistance < leftChild.radius ||
      rightQueryCost - candidates.maxDistance < rightChild.radius) {
      val remainingChild = {
        if (leftQueryCost <= rightQueryCost) {
          leftChild.query(candidates)
          rightChild
        } else {
          rightChild.query(candidates)
          leftChild
        }
      }
      // check again to see if the remaining child is still worth looking
      if (candidates.notFull ||
        remainingChild.distance(candidates) - candidates.maxDistance < remainingChild.radius) {
        remainingChild.query(candidates)
      }
    }
    candidates
  }
}

object MetricTree {
  /**
   * Build a (metric)[[Tree]] that facilitate k-NN query
   *
   * @param data vectors that contain all training data
   * @param seed random number generator seed used in pivot point selecting
   * @return a [[Tree]] can be used to do k-NN query
   */
  def build(data: IndexedSeq[RowWithVector], leafSize: Int = 1, seed: Long = 0L): Tree = {
    val size = data.size
    if (size == 0) {
      Empty
    } else if (size <= leafSize) {
      Leaf(data)
    } else {
      val rand = new XORShiftRandom(seed)
      val randomPivot = data(rand.nextInt(size)).vector
      val leftPivot = data.maxBy(v => randomPivot.fastSquaredDistance(v.vector)).vector
      if (leftPivot == randomPivot) {
        // all points are identical (or only one point left)
        Leaf(data, randomPivot, 0.0)
      } else {
        val rightPivot = data.maxBy(v => leftPivot.fastSquaredDistance(v.vector)).vector
        val pivot = new VectorWithNorm(Vectors.
          fromBreeze((leftPivot.vector.asBreeze + rightPivot.vector.asBreeze) / 2.0))
        val radius = math.sqrt(data.map(v => pivot.fastSquaredDistance(v.vector)).max)
        val (leftPartition, rightPartition) = data.partition {
          v => leftPivot.fastSquaredDistance(v.vector) < rightPivot.fastSquaredDistance(v.vector)
        }

        MetricTree(
          build(leftPartition, leafSize, rand.nextLong()),
          leftPivot,
          build(rightPartition, leafSize, rand.nextLong()),
          rightPivot,
          pivot,
          radius
        )
      }
    }
  }
}

/**
 * A [[SpillTree]] represents a SpillNode. Just like [[MetricTree]], it splits data into
 * two partitions.
 * However, instead of partition data into exactly two halves, it contains a buffer zone with
 * size of tau.
 * Left child contains all data left to the center plane + tau
 * (in the leftPivot -> rightPivot direction).
 * Right child contains all data right to the center plane - tau.
 *
 * Search doesn't do backtracking but rather adopt a defeatist search where it search
 * the most prominent
 * child and that child only. The buffer ensures such strategy doesn't result in a poor outcome.
 */
case class SpillTree(leftChild: Tree,
                     leftPivot: VectorWithNorm,
                     rightChild: Tree,
                     rightPivot: VectorWithNorm,
                     pivot: VectorWithNorm,
                     radius: Double,
                     tau: Double,
                     bufferSize: Int
                    ) extends Tree {
  override val size = leftChild.size + rightChild.size - bufferSize
  override val leafCount = leftChild.leafCount + rightChild.leafCount

  override def iterator: Iterator[RowWithVector] =
    leftChild.iterator ++ rightChild.iterator.filter(childFilter(leftPivot, rightPivot))

  override def query(candidates: KNNCandidates): KNNCandidates = {
    if (size <= candidates.k - candidates.candidates.size) {
      iterator.foreach(candidates.insert)
    } else {
      val leftQueryCost = candidates.queryVector.fastSquaredDistance(leftPivot)
      val rightQueryCost = candidates.queryVector.fastSquaredDistance(rightPivot)

      (if (leftQueryCost <= rightQueryCost) leftChild else rightChild).query(candidates)

      // fill candidates with points from other child excluding buffer so we don't double count.
      // depending on K and how high we are in the tree, this can be very expensive and undesirable
      if (candidates.notFull) {
        (if (leftQueryCost <= rightQueryCost) {
          rightChild.iterator.filter(childFilter(leftPivot, rightPivot))
        } else {
          leftChild.iterator.filter(childFilter(rightPivot, leftPivot))
        }).foreach(candidates.tryInsert)
      }
    }
    candidates
  }

  private[this] val childFilter: (VectorWithNorm, VectorWithNorm) => RowWithVector => Boolean =
    (p1, p2) => p => p.vector.fastDistance(p1) - p.vector.fastDistance(p2) > tau
}

object SpillTree {
  /**
   * Build a (spill)[[Tree]] that facilitate k-NN query
   *
   * @param data vectors that contain all training data
   * @param tau  overlapping size
   * @param seed random number generators seed used in pivot point selecting
   * @return a [[Tree]] can be used to do k-NN query
   */
  def build(data: IndexedSeq[RowWithVector], leafSize: Int = 1, tau: Double, seed: Long = 0L):
  Tree = {
    val size = data.size
    if (size == 0) {
      Empty
    } else if (size <= leafSize) {
      Leaf(data)
    } else {
      val rand = new XORShiftRandom(seed)
      val randomPivot = data(rand.nextInt(size)).vector
      val leftPivot = data.maxBy(v => randomPivot.fastSquaredDistance(v.vector)).vector
      if (leftPivot == randomPivot) {
        // all points are identical (or only one point left)
        Leaf(data, randomPivot, 0.0)
      } else {
        val rightPivot = data.maxBy(v => leftPivot.fastSquaredDistance(v.vector)).vector
        val pivot = new VectorWithNorm(Vectors.
          fromBreeze((leftPivot.vector.asBreeze + rightPivot.vector.asBreeze) / 2.0))
        val radius = math.sqrt(data.map(v => pivot.fastSquaredDistance(v.vector)).max)
        val dataWithDistance = data.map(v =>
          (v, leftPivot.fastDistance(v.vector), rightPivot.fastDistance(v.vector))
        )
        val leftPartition = dataWithDistance.filter { case (_, left, right) => left - right <= tau }.map(_._1)
        val rightPartition = dataWithDistance.filter { case (_, left, right) => right - left <= tau }.map(_._1)

        SpillTree(
          build(leftPartition, leafSize, tau, rand.nextLong()),
          leftPivot,
          build(rightPartition, leafSize, tau, rand.nextLong()),
          rightPivot,
          pivot,
          radius,
          tau,
          leftPartition.size + rightPartition.size - size
        )
      }
    }
  }
}

object HybridTree {
  /**
   * Build a (hybrid-spill)[[Tree]] that facilitate k-NN query
   *
   * @param data vectors that contain all training data
   * @param seed random number generator seed used in pivot point selecting
   * @param tau  overlapping size
   * @param rho  balance threshold
   * @return a [[Tree]] can be used to do k-NN query
   */
  def build(data: IndexedSeq[RowWithVector],
            leafSize: Int = 1,
            tau: Double,
            rho: Double = 0.7,
            seed: Long = 0L): Tree = {
    val size = data.size
    if (size == 0) {
      Empty
    } else if (size <= leafSize) {
      Leaf(data)
    } else {
      val rand = new XORShiftRandom(seed)
      val randomPivot = data(rand.nextInt(size)).vector
      val leftPivot = data.maxBy(v => randomPivot.fastSquaredDistance(v.vector)).vector
      if (leftPivot == randomPivot) {
        // all points are identical (or only one point left)
        Leaf(data, randomPivot, 0.0)
      } else {
        val rightPivot = data.maxBy(v => leftPivot.fastSquaredDistance(v.vector)).vector
        val pivot = new VectorWithNorm(Vectors.
          fromBreeze((leftPivot.vector.asBreeze + rightPivot.vector.asBreeze) / 2.0))
        val radius = math.sqrt(data.map(v => pivot.fastSquaredDistance(v.vector)).max)
        lazy val dataWithDistance = data.map(v =>
          (v, leftPivot.fastDistance(v.vector), rightPivot.fastDistance(v.vector))
        )
        // implemented boundary is parabola (rather than perpendicular plane described in the paper)
        lazy val leftPartition = dataWithDistance.filter { case (_, left, right) => left - right <= tau }.map(_._1)
        lazy val rightPartition = dataWithDistance.filter { case (_, left, right) => right - left <= tau }.map(_._1)

        if (rho <= 0.0 || leftPartition.size > size * rho || rightPartition.size > size * rho) {
          val (leftPartition, rightPartition) = data.partition {
            v => leftPivot.fastSquaredDistance(v.vector) < rightPivot.fastSquaredDistance(v.vector)
          }
          MetricTree(
            build(leftPartition, leafSize, tau, rho, rand.nextLong()),
            leftPivot,
            build(rightPartition, leafSize, tau, rho, rand.nextLong()),
            rightPivot,
            pivot,
            radius
          )
        } else {
          SpillTree(
            build(leftPartition, leafSize, tau, rho, rand.nextLong()),
            leftPivot,
            build(rightPartition, leafSize, tau, rho, rand.nextLong()),
            rightPivot,
            pivot,
            radius,
            tau,
            leftPartition.size + rightPartition.size - size
          )
        }
      }
    }
  }
}

/**
 * Structure to maintain search progress/results for a single query vector.
 * Internally uses a PriorityQueue to maintain a max-heap to keep track of the
 * next neighbor to evict.
 *
 * @param queryVector vector being searched
 * @param k           number of neighbors to return
 */
class KNNCandidates(val queryVector: VectorWithNorm, val k: Int) extends Serializable {
  private[knn] val candidates = mutable.PriorityQueue.empty[(RowWithVector, Double)] {
    Ordering.by(_._2)
  }

  // return the current maximum distance from neighbor to search vector
  def maxDistance: Double = if (candidates.isEmpty) 0.0 else candidates.head._2

  // insert evict neighbor if required. however it doesn't make sure the insert improves
  // search results. it is caller's responsibility to make sure either candidate list
  // is not full or the inserted neighbor brings the maxDistance down
  def insert(v: RowWithVector, d: Double): Unit = {
    while (candidates.size >= k) candidates.dequeue()
    candidates.enqueue((v, d))
  }

  def insert(v: RowWithVector): Unit = insert(v, v.vector.fastDistance(queryVector))

  def tryInsert(v: RowWithVector): Unit = {
    val distance = v.vector.fastDistance(queryVector)
    if (notFull || distance < maxDistance) insert(v, distance)
  }

  def toIterable: Iterable[(RowWithVector, Double)] = candidates

  def notFull: Boolean = candidates.size < k
}



