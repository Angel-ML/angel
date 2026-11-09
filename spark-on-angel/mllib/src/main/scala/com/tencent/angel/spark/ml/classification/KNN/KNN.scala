package org.apache.spark.ml.classification.knn

import breeze.linalg.{DenseVector, Vector => BV}
import breeze.stats._
import org.apache.spark.ml.classification.knn.KNN.{KNNPartitioner, RowWithVector, VectorWithNorm}
import org.apache.spark.ml.linalg.{Vector, VectorUDT, Vectors}
import org.apache.spark.ml.param._
import org.apache.spark.ml.param.shared._
import org.apache.spark.ml.util._
import org.apache.spark.ml.{Estimator, Model}
import org.apache.spark.mllib.rdd.MLPairRDDFunctions._
import org.apache.spark.rdd.{RDD, ShuffledRDD}
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Dataset, Row}
import org.apache.spark.storage.StorageLevel
import org.apache.spark.util.random.XORShiftRandom
import org.apache.spark.{HashPartitioner, Partitioner}

import scala.annotation.tailrec
import scala.collection.mutable.ArrayBuffer
import scala.util.hashing.byteswap64


trait KNNModelParams extends Params with HasFeaturesCol with HasLabelCol {
  /**
   * Param for the column name for returned neighbors.
   * Default: "neighbors"
   */
  val neighborsCol = new Param[String](this, "neighborsCol", "column names for returned neighbors")

  def getNeighborsCol: String = $(neighborsCol)

  /**
   * Param for number of neighbors to find (> 0).
   * Default: 5
   */
  val k = new IntParam(this, "k", "number of neighbors to find", ParamValidators.gt(0))

  def getK: Int = $(k)

  /**
   * Param for maximum distance to find neighbors
   * Default: Double.PositiveInfinity
   *
   */
  val maxDistance = new DoubleParam(this, "maxNeighbors", "maximum distance to find neighbors",
    ParamValidators.gt(0))

  def getMaxDistance: Double = $(maxDistance)


  /**
   * Param for size of buffer used to construct spill trees and top-level tree search.
   * Note the buffer size is 2 * tau as described in the paper.
   *
   * When buffer size is 0.0, the tree itself reverts to a metric tree.
   * -1.0 triggers automatic effective nearest neighbor distance estimation.
   *
   * Default: -1.0
   */
  val bufferSize = new DoubleParam(this, "bufferSize",
    "size of buffer used to construct spill trees and top-level tree search",
    ParamValidators.gtEq(-1.0))

  def getBufferSize: Double = $(bufferSize)

  def transform(data: RDD[Vector], topTree: Tree, subTrees: Array[Tree]):
  RDD[(Long, Array[Row])] = {
    val subTreesRDD = data.context.parallelize(subTrees, subTrees.length)
    val searchData = data.zipWithIndex()
      .flatMap {
        case (vector, index) =>
          val vectorWithNorm = new VectorWithNorm(vector)
          val idx = KNN.searchIndices(vectorWithNorm, topTree, $(bufferSize))
            .map(i => (i, (vectorWithNorm, index)))

          assert(idx.nonEmpty, s"indices must be non-empty: $vector ($index)")
          idx
      }
      .partitionBy(new HashPartitioner(subTrees.length))

    // for each partition, search points within corresponding child tree
    val results = searchData.zipPartitions(subTreesRDD) {
      (childData, trees) =>
        val tree = trees.next()
        assert(!trees.hasNext)
        childData.flatMap {
          case (_, (point, i)) =>
            tree.query(point, $(k)).collect {
              case (neighbor, distance) if distance <= $(maxDistance) =>
                (i, (neighbor.row, distance))
            }
        }
    }

    // merge results by point index together and keep topK results
    results.topByKey($(k))(Ordering.by(-_._2))
      .map { case (i, seq) => (i, seq.map(_._1)) }
  }

  def transform(dataset: Dataset[_], topTree: Tree, subTrees: Array[Tree]):
  RDD[(Long, Array[Row])] = {
    transform(dataset.select($(featuresCol)).rdd.map(_.getAs[Vector](0)), topTree, subTrees)
  }
}

trait KNNParams extends KNNModelParams with HasSeed {
  /**
   * Param for number of points to sample for top-level tree (> 0).
   * Default: 1000
   */
  val topTreeSize = new IntParam(this, "topTreeSize",
    "number of points to sample for top-level tree", ParamValidators.gt(0))

  def getTopTreeSize: Int = $(topTreeSize)

  /**
   * Param for number of points at which to switch to brute-force for top-level tree (> 0).
   * Default: 5
   *
   * @group param
   */
  val topTreeLeafSize = new IntParam(this, "topTreeLeafSize",
    "number of points at which to switch to brute-force for top-level tree", ParamValidators.gt(0))

  /** @group getParam */
  def getTopTreeLeafSize: Int = $(topTreeLeafSize)

  /**
   * Param for number of points at which to switch to brute-force for distributed sub-trees (> 0).
   * Default: 20
   *
   */
  val subTreeLeafSize = new IntParam(this, "subTreeLeafSize",
    "number of points at which to switch to brute-force for distributed sub-trees",
    ParamValidators.gt(0))

  def getSubTreeLeafSize: Int = $(subTreeLeafSize)

  /**
   * Param for number of sample sizes to take when estimating buffer size (at least two samples).
   * Default: 100 to 1000 by 100
   */
  val bufferSizeSampleSizes = new IntArrayParam(this, "bufferSizeSampleSize",
    "number of sample sizes to take when estimating buffer size",
    { arr: Array[Int] => arr.length > 1 && arr.forall(_ > 0) })

  def getBufferSizeSampleSizes: Array[Int] = $(bufferSizeSampleSizes)

  /**
   * Param for fraction of total points at which spill tree reverts back to metric tree
   * if either child contains more points (0 <= rho <= 1).
   * Default: 70%
   */
  val balanceThreshold = new DoubleParam(this, "balanceThreshold",
    "fraction of total points at which spill tree reverts back" +
      " to metric tree if either child contains more points",
    ParamValidators.inRange(0, 1))

  def getBalanceThreshold: Double = $(balanceThreshold)

  setDefault(topTreeSize -> 1000, topTreeLeafSize -> 10, subTreeLeafSize -> 30,
    bufferSize -> -1.0, bufferSizeSampleSizes -> (100 to 1000 by 100).toArray,
    balanceThreshold -> 0.7, k -> 5, neighborsCol -> "neighbors",
    maxDistance -> Double.PositiveInfinity)

  /**
   * Validates and transforms the input schema.
   *
   * @param schema input schema
   * @return output schema
   */
  protected def validateAndTransformSchema(schema: StructType): StructType = {
    SchemaUtils.checkColumnType(schema, $(featuresCol), new VectorUDT)
    val auxFeatures = $(labelCol).map(c => schema(c))
    SchemaUtils.appendColumn(schema, $(neighborsCol), ArrayType(StructType(auxFeatures)))
  }
}

/**
 * kNN Model facilitates k-Nestrest Neighbor search by storing distributed hybrid spill tree.
 * Top level tree is a MetricTree but instead of using back tracking, it searches all possible
 * leaves in parallel to avoid multiple iterations. It uses the same buffer size that is used in
 * model training, when the search vector falls into the buffer zone of the node, it dispatches
 * search to both children.
 *
 * A high level overview of the search phases is as follows:
 *
 *  1. For each vector to search, go through the top level tree to output a pair of (index, point)
 *  1. Repartition search points by partition index
 *  1. Search each point through the hybrid spill tree in that particular partition
 *  1. For each point, merge results from different partitions and keep top k results.
 *
 */

class KNNModel(override val uid: String, val topTree: Tree, val subTrees: Array[Tree])
  extends Model[KNNModel] with KNNModelParams {

  def setNeighborsCol(value: String): this.type = set(neighborsCol, value)

  def setK(value: Int): this.type = set(k, value)

  def setMaxDistance(value: Double): this.type = set(maxDistance, value)

  def setBufferSize(value: Double): this.type = set(bufferSize, value)

  override def transform(dataset: Dataset[_]): DataFrame = {
    val merged = transform(dataset, topTree, subTrees)

    dataset.sqlContext.createDataFrame(
      dataset.rdd.zipWithIndex().map { case (row, i) => (i, row) }
        .leftOuterJoin(merged)
        .map {
          case (i, (row, neighbors)) =>
            Row.fromSeq(row.asInstanceOf[Row].toSeq :+ neighbors.getOrElse(Array.empty))
        },
      transformSchema(dataset.schema)
    )
  }

  override def transformSchema(schema: StructType): StructType = {
    val auxFeatures = $(labelCol).map(c => schema(c))
    SchemaUtils.appendColumn(schema, $(neighborsCol), ArrayType(StructType(auxFeatures)))
  }

  override def copy(extra: ParamMap): KNNModel = {
    val copied = new KNNModel(uid, topTree, subTrees)
    copyValues(copied, extra).setParent(parent)
  }

  def toNewClassificationModel(uid: String, numClasses: Int): KNNClassificationModel = {
    copyValues(new KNNClassificationModel(uid, topTree, subTrees, numClasses))
  }
}

/**
 * k-Nearest Neighbors (kNN) algorithm
 *
 * kNN finds k closest observations in training dataset. It can be used for both classification
 * and regression.
 * Furthermore it can also be used for other purposes such as input to clustering algorithm.
 *
 * While the brute-force approach requires no pre-training, each prediction requires going
 * through the entire training
 * set resulting O(n log(k)) runtime per individual prediction using a heap keep track of
 * neighbor candidates.
 * Many different implementations have been proposed such as Locality Sensitive Hashing (LSH),
 * KD-Tree, Metric Tree and etc.
 * Each algorithm has its shortcomings that prevent them to be effective on large-scale and/or
 * high-dimensional dataset.
 *
 * This is an implementation of kNN based upon distributed Hybrid Spill-Trees where training
 * points are organized into distributed binary trees. The algorithm is designed to support
 * accurate approximate kNN search but by tuning parameters an exact search can also be
 * performed with cost of additional runtime.
 *
 * Each binary tree node is either a
 *
 * '''Metric Node''':
 * Metric Node partition points exclusively into two children by finding two pivot points
 * and divide by middle plane.
 * When searched, the child whose pivot is closer to query vector is searched first. Back
 * tracking is required to
 * ensure accuracy in this case, where the other child should be searched if it can possibly
 * contain better neighbor
 * based upon candidates picked during previous search.
 *
 * '''Spill Node''':
 * Spill Node also partitions points into two children however there are an overlapping buffer
 * between the two pivot
 * points. The larger the buffer size, the less effective the node eliminates points thus could
 * increase tree height.
 * When searched, defeatist search is used where only one child is searched and no back tracking
 * happens in this
 * process. Because of the buffer between two children, we are likely to end up with good enough
 * candidates without
 * searching the other part of the tree.
 *
 * While Spill Node promises O(h) runtime where h is the tree height, the tree is deeper than
 * Metric Tree's O(log n)
 * height on average. Furthermore, when it comes down to leaves where points are more closer
 * to each other, the static
 * buffer size means more points will end up in the buffer. Therefore a Balance Threshold (rho)
 * is introduced: when
 * either child of Spill Node makes up more than rho fraction of the total points at this level,
 * Spill Node is reverted
 * back to a Metric Node.
 *
 * A high level overview of the algorithm is as follows:
 *
 *  1. Sample M data points (M is relatively small and can be held in driver)
 *  2. Build the top level metric tree
 *  3. Repartition RDD by assigning each point to leaf node of the above tree
 *  4. Build a hybrid spill tree at each partition
 *
 * This concludes the training phase of kNN.
 * See [[KNNModel]] for details on prediction phase.
 *
 *
 * This algorithm is described in [[http://dx.doi.org/10.1109/WACV.2007.18]] where it was
 * shown to scale well in terms of
 * number of observations and dimensions, bounded by the available memory across clusters
 * (billions in paper's example).
 * This implementation adapts the MapReduce algorithm to work with Spark.
 *
 */

class KNN(override val uid: String) extends Estimator[KNNModel] with KNNParams {

  def this() = this(Identifiable.randomUID("knn"))

  def setFeaturesCol(value: String): this.type = set(featuresCol, value)

  def setLabelCol(value: String): this.type = set(labelCol, value)

  def setK(value: Int): this.type = set(k, value)

  def setTopTreeSize(value: Int): this.type = set(topTreeSize, value)

  def setTopTreeLeafSize(value: Int): this.type = set(topTreeLeafSize, value)

  def setSubTreeLeafSize(value: Int): this.type = set(subTreeLeafSize, value)

  def setBufferSizeSampleSizes(value: Array[Int]): this.type = set(bufferSizeSampleSizes, value)

  def setBalanceThreshold(value: Double): this.type = set(balanceThreshold, value)

  def setSeed(value: Long): this.type = set(seed, value)

  override def fit(dataset: Dataset[_]): KNNModel = {

    val rand = new XORShiftRandom($(seed))
    // prepare data for model estimation
    val data = dataset.select($(featuresCol), $(labelCol)).rdd
      .map { case Row(features: Vector, label) => new RowWithVector(features,
        Row(label))
      }
    // sample data to build top-level tree
    val sampled = data.sample(false, $(topTreeSize).toDouble / dataset.count(),
      rand.nextLong()).collect()
    val topTree = MetricTree.build(sampled, $(topTreeLeafSize), rand.nextLong())
    // build partitioner using top-level tree
    val part = new KNNPartitioner(topTree)
    // noinspection ScalaStyle
    val repartitioned = new ShuffledRDD[RowWithVector, Null, Null](data.map(v => (v, null)),
      part).keys

    val tau = if ($(balanceThreshold) > 0 && $(bufferSize) < 0) {
      KNN.estimateTau(data, $(bufferSizeSampleSizes), rand.nextLong())
    } else {
      math.max(0, $(bufferSize))
    }

    val trees = repartitioned.mapPartitionsWithIndex {
      (partitionId, itr) =>
        val rand = new XORShiftRandom(byteswap64($(seed) ^ partitionId))
        val childTree =
          HybridTree.build(itr.toIndexedSeq, $(subTreeLeafSize), tau,
            $(balanceThreshold), rand.nextLong())

        Iterator(childTree)
    }.persist(StorageLevel.MEMORY_AND_DISK)
    //    trees.count()

    val model = new KNNModel(uid, topTree, trees.collect()).setParent(this)
    copyValues(model).setBufferSize(tau)
  }

  override def transformSchema(schema: StructType): StructType = {
    validateAndTransformSchema(schema)
  }

  override def copy(extra: ParamMap): KNN = defaultCopy(extra)
}

object KNN {

  /**
   * VectorWithNorm can use more efficient algorithm to calculate distance
   */
  case class VectorWithNorm(vector: Vector, norm: Double) {
    def this(vector: Vector) = this(vector, Vectors.norm(vector, 2))

    def this(vector: BV[Double]) = this(Vectors.fromBreeze(vector))

    def fastSquaredDistance(v: VectorWithNorm): Double = {
      Vectors.sqdist(v.vector, vector)
    }

    def fastDistance(v: VectorWithNorm): Double = math.sqrt(fastSquaredDistance(v))
  }

  /**
   * VectorWithNorm plus auxiliary row information
   */
  case class RowWithVector(vector: VectorWithNorm, row: Row) {
    def this(vector: Vector, row: Row) = this(new VectorWithNorm(vector), row)
  }

  /**
   * Estimate a suitable buffer size based on dataset
   *
   * A suitable buffer size is the minimum size such that nearest neighbors can be accurately
   * found even at boundary of splitting plane between pivot points. Therefore assuming points
   * are uniformly distributed in high dimensional space, it should be approximately the average
   * distance between points.
   *
   * Specifically the number of points within a certain radius of a given point is proportionally
   * to the density of points raised to the effective number of dimensions, of which manifold
   * data points exist on:
   * R_s = \frac{c}{N_s ** 1/d}
   * where R_s is the radius, N_s is the number of points, d is effective number of dimension,
   * and c is a constant.
   *
   * To estimate R_s_all for entire dataset, we can take samples of the dataset of different size
   * N_s to compute R_s.
   * We can estimate c and d using linear regression. Lastly we can calculate R_s_all using total
   * number of observation in dataset.
   *
   */
  def estimateTau(data: RDD[RowWithVector], sampleSize: Array[Int], seed: Long): Double = {
    val total = data.count()

    // take samples of points for estimation
    val samples = data.mapPartitionsWithIndex {
      case (partitionId, itr) =>
        val rand = new XORShiftRandom(byteswap64(seed ^ partitionId))
        itr.flatMap {
          p =>
            sampleSize.zipWithIndex
              .filter { case (size, _) => rand.nextDouble() * total < size }
              .map { case (size, index) => (index, p) }
        }
    }
    // compute N_s and R_s pairs
    val estimators = samples
      .groupByKey()
      .map {
        case (index, points) => (points.size, computeAverageDistance(points))
      }.collect().distinct

    // collect x and y vectors
    val x = DenseVector(estimators.map { case (n, _) => math.log(n) })
    val y = DenseVector(estimators.map { case (_, d) => math.log(d) })

    // estimate log(R_s) = alpha + beta * log(N_s)
    val xMeanVariance  = meanAndVariance(x)
    val xmean = xMeanVariance.mean
    val yMeanVariance = meanAndVariance(y)
    val ymean = yMeanVariance.mean

    val corr = (mean(x *:* y) - xmean * ymean) / math.sqrt((mean(x *:* x) - xmean * xmean) *
      (mean(y *:* y) - ymean * ymean))

    val beta = corr * yMeanVariance.stdDev / xMeanVariance.stdDev
    val alpha = ymean - beta * xmean
    val rs = math.exp(alpha + beta * math.log(total))

    if (beta > 0) {
      val yMax = breeze.linalg.max(y)
      yMax
    } else {
      // c = alpha, d = - 1 / beta
      rs / math.sqrt(-1 / beta)
    }
  }

  // compute the average distance of nearest neighbors within points using brute-force
  private[this] def computeAverageDistance(points: Iterable[RowWithVector]): Double = {
    val distances = points.map {
      point => points.map(p => p.vector.fastSquaredDistance(point.vector)).filter(_ > 0).min
    }.map(math.sqrt)

    distances.sum / distances.size
  }

  /**
   * Search leaf index used by KNNPartitioner to partition training points
   *
   * @param v    one training point to partition
   * @param tree top tree constructed using sampled points
   * @param acc  accumulator used to help determining leaf index
   * @return leaf/partition index
   */
  @tailrec
  private[knn] def searchIndex(v: RowWithVector, tree: Tree, acc: Int = 0): Int = {
    tree match {
      case node: MetricTree =>
        val leftDistance = node.leftPivot.fastSquaredDistance(v.vector)
        val rightDistance = node.rightPivot.fastSquaredDistance(v.vector)
        if (leftDistance < rightDistance) {
          searchIndex(v, node.leftChild, acc)
        } else {
          searchIndex(v, node.rightChild, acc + node.leftChild.leafCount)
        }
      case _ => acc // reached leaf
    }
  }

  def searchIndices(v: VectorWithNorm, tree: Tree, tau: Double, acc: Int = 0): Seq[Int] = {
    tree match {
      case node: MetricTree =>
        val leftDistance = node.leftPivot.fastDistance(v)
        val rightDistance = node.rightPivot.fastDistance(v)

        val buffer = new ArrayBuffer[Int]
        if (leftDistance - rightDistance <= tau) {
          buffer ++= searchIndices(v, node.leftChild, tau, acc)
        }

        if (rightDistance - leftDistance <= tau) {
          buffer ++= searchIndices(v, node.rightChild, tau, acc + node.leftChild.leafCount)
        }

        buffer
      case _ => Seq(acc) // reached leaf
    }
  }

  /**
   * Partitioner used to map vector to leaf node which determines the partition it goes to
   *
   * @param tree [[MetricTree]] used to find leaf
   */
  class KNNPartitioner[T <: RowWithVector](tree: Tree) extends Partitioner {
    override def numPartitions: Int = tree.leafCount

    override def getPartition(key: Any): Int = {
      key match {
        case v: RowWithVector => searchIndex(v, tree)
        case _ => throw new Exception(s"Key must be of type Vector but got: $key")
      }
    }
  }

}

