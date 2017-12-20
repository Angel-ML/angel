/*
 * Tencent is pleased to support the open source community by making Angel available.
 *
 * Copyright (C) 2017 THL A29 Limited, a Tencent company. All rights reserved.
 *
 * Licensed under the BSD 3-Clause License (the "License"); you may not use this file except in
 * compliance with the License. You may obtain a copy of the License at
 *
 * https://opensource.org/licenses/BSD-3-Clause
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */

package org.apache.spark.ml.clustering.ps

import scala.collection.mutable.ArrayBuffer

import org.apache.hadoop.fs.Path
import org.apache.spark.SparkException
import org.apache.spark.annotation.{Experimental, Since}
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.ml.clustering.ClusteringSummary
import org.apache.spark.ml.linalg.{Vector, VectorUDT, Vectors}
import org.apache.spark.ml.param._
import org.apache.spark.ml.param.shared._
import org.apache.spark.ml.util._
import org.apache.spark.ml.{Estimator, Model}
import org.apache.spark.mllib.linalg.BLAS.{axpy, dot, scal}
import org.apache.spark.mllib.linalg.VectorImplicits._
import org.apache.spark.mllib.linalg.{SparseVector, Vector => OldVector, Vectors => OldVectors}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.functions.{col, udf}
import org.apache.spark.sql.types.{IntegerType, StructType}
import org.apache.spark.sql.{DataFrame, Dataset, Row}
import org.apache.spark.storage.StorageLevel
import org.apache.spark.util.VersionUtils.majorVersion
import org.apache.spark.util.random.XORShiftRandom

import com.tencent.angel.spark.context.PSContext
import com.tencent.angel.spark.models.vector.PSVector
import com.tencent.angel.spark.models.vector.enhanced.BreezePSVector

/**
  * Common params for KMeans and KMeansModel
  */
private[clustering] trait KMeansParams extends Params with HasMaxIter with HasFeaturesCol
  with HasSeed with HasPredictionCol with HasTol {

  /**
    * The number of clusters to create (k). Must be &gt; 1. Note that it is possible for fewer than
    * k clusters to be returned, for example, if there are fewer than k distinct points to cluster.
    * Default: 2.
    *
    * @group param
    */
  @Since("1.5.0")
  final val k = new IntParam(this, "k", "The number of clusters to create. " +
    "Must be > 1.", ParamValidators.gt(1))

  /** @group getParam */
  @Since("1.5.0")
  def getK: Int = $(k)

  setDefault(k->5)

  /**
    * Param for the initialization algorithm. This can be either "random" to choose random points as
    * initial cluster centers, or "k-means||" to use a parallel variant of k-means++
    * (Bahmani et al., Scalable K-Means++, VLDB 2012). Default: k-means||.
    *
    * @group expertParam
    */
  @Since("1.5.0")
  final val initMode = new Param[String](this, "initMode", "The initialization algorithm. " +
    "Supported options: 'random' and 'k-means||'.",
    (value: String) => KMeans.validateInitMode(value))

  /** @group expertGetParam */
  @Since("1.5.0")
  def getInitMode: String = $(initMode)

  /**
    * Param for the number of steps for the k-means|| initialization mode. This is an advanced
    * setting -- the default of 2 is almost always enough. Must be &gt; 0. Default: 2.
    *
    * @group expertParam
    */
  @Since("1.5.0")
  final val initSteps = new IntParam(this, "initSteps", "The number of steps for k-means|| " +
    "initialization mode. Must be > 0.", ParamValidators.gt(0))

  /** @group expertGetParam */
  @Since("1.5.0")
  def getInitSteps: Int = $(initSteps)

  /**
    * Validates and transforms the input schema.
    *
    * @param schema input schema
    * @return output schema
    */
  protected def validateAndTransformSchema(schema: StructType): StructType = {
    SchemaUtils.checkColumnType(schema, $(featuresCol), new VectorUDT)
    SchemaUtils.appendColumn(schema, $(predictionCol), IntegerType)
  }
}


/**
  * K-means clustering with support for k-means|| initialization proposed by Bahmani et al.
  *
  * @see <a href="http://dx.doi.org/10.14778/2180912.2180915">Bahmani et al., Scalable k-means++.</a>
  */
@Since("1.5.0")
class KMeans @Since("1.5.0")(@Since("1.5.0") override val uid: String)
  extends Estimator[KMeansModel] with KMeansParams with DefaultParamsWritable {

  setDefault(
    k -> 2,
    maxIter -> 20,
    initMode -> KMeans.K_MEANS_PARALLEL,
    initSteps -> 2,
    tol -> 1e-4)

  @Since("1.5.0")
  override def copy(extra: ParamMap): KMeans = defaultCopy(extra)

  @Since("1.5.0")
  def this() = this(Identifiable.randomUID("kmeans"))

  /** @group setParam */
  @Since("1.5.0")
  def setFeaturesCol(value: String): this.type = set(featuresCol, value)

  /** @group setParam */
  @Since("1.5.0")
  def setPredictionCol(value: String): this.type = set(predictionCol, value)

  /** @group setParam */
  @Since("1.5.0")
  def setK(value: Int): this.type = set(k, value)

  /** @group expertSetParam */
  @Since("1.5.0")
  def setInitMode(value: String): this.type = set(initMode, value)

  /** @group expertSetParam */
  @Since("1.5.0")
  def setInitSteps(value: Int): this.type = set(initSteps, value)

  /** @group setParam */
  @Since("1.5.0")
  def setMaxIter(value: Int): this.type = set(maxIter, value)

  /** @group setParam */
  @Since("1.5.0")
  def setTol(value: Double): this.type = set(tol, value)

  /** @group setParam */
  @Since("1.5.0")
  def setSeed(value: Long): this.type = set(seed, value)


  /**
    * Initialize a set of cluster centers at random.
    */
  private def initRandom(data: RDD[VectorWithNorm], psVector: PSVector): Array[PsVectorWithNorm] = {
    // Select without replacement; may still produce duplicates if the data has < k distinct
    // points, so deduplicate the centroids to match the behavior of k-means|| in the same situation
    data.takeSample(false, $(k), new XORShiftRandom($(seed)).nextInt())
      .map(_.vector).distinct.map(v => {
      val initPSCenter = PSVector.duplicate(psVector)
      initPSCenter.push(v.toArray)
      new PsVectorWithNorm(initPSCenter.toBreeze)
    })
  }


  /**
    * Initialize a set of cluster centers using the k-means|| algorithm by Bahmani et al.
    * (Bahmani et al., Scalable K-Means++, VLDB 2012). This is a variant of k-means++ that tries
    * to find dissimilar cluster centers by starting with a random center and then doing
    * passes where more centers are chosen with probability proportional to their squared distance
    * to the current cluster set. It results in a provable approximation to an optimal clustering.
    *
    * The original paper can be found at http://theory.stanford.edu/~sergei/papers/vldb12-kmpar.pdf.
    */
  private[clustering] def initKMeansParallel(data: RDD[VectorWithNorm]): Array[VectorWithNorm] = {
    // Initialize empty centers and point costs.
    var costs = data.map(_ => Double.PositiveInfinity)

    // Initialize the first center to a random point.
    val seed1 = new XORShiftRandom($(seed)).nextInt()
    val sample = data.takeSample(false, 1, seed1)
    // Could be empty if data is empty; fail with a better message early:
    require(sample.nonEmpty, s"No samples available from $data")

    val centers = ArrayBuffer[VectorWithNorm]()
    var newCenters = Seq(sample.head.toDense)
    centers ++= newCenters

    // On each step, sample 2 * k points on average with probability proportional
    // to their squared distance from the centers. Note that only distances between points
    // and new centers are computed in each iteration.
    var step = 0
    var bcNewCentersList = ArrayBuffer[Broadcast[_]]()
    while (step < $(initSteps)) {
      val bcNewCenters = data.context.broadcast(newCenters)
      bcNewCentersList += bcNewCenters
      val preCosts = costs
      costs = data.zip(preCosts).map { case (point, cost) =>
        math.min(KMeans.pointCost(bcNewCenters.value, point), cost)
      }.persist(StorageLevel.MEMORY_AND_DISK)
      val sumCosts = costs.sum()

      bcNewCenters.unpersist(blocking = false)
      preCosts.unpersist(blocking = false)

      val chosen = data.zip(costs).mapPartitionsWithIndex { (index, pointCosts) =>
        val rand = new XORShiftRandom(seed1 ^ (step << 16) ^ index)
        pointCosts.filter { case (_, c) => rand.nextDouble() < 2.0 * c * $(k) / sumCosts }.map(_._1)
      }.collect()
      newCenters = chosen.map(_.toDense)
      centers ++= newCenters
      step += 1
    }

    costs.unpersist(blocking = false)
    bcNewCentersList.foreach(_.destroy(false))

    val distinctCenters = centers.map(_.vector).distinct.map(new VectorWithNorm(_))

    if (distinctCenters.size <= $(k)) {
      distinctCenters.toArray
    } else {
      // Finally, we might have a set of more than k distinct candidate centers; weight each
      // candidate by the number of points in the dataset mapping to it and run a local k-means++
      // on the weighted centers to pick k of them
      val bcCenters = data.context.broadcast(distinctCenters)
      val countMap = data.map(KMeans.findClosest(bcCenters.value, _)._1).countByValue()

      bcCenters.destroy(blocking = false)

      val myWeights = distinctCenters.indices.map(countMap.getOrElse(_, 0L).toDouble).toArray
      LocalKMeans.kMeansPlusPlus(0, distinctCenters.toArray, myWeights, $(k), 30)
    }
  }


  @Since("2.0.0")
  override def fit(dataset: Dataset[_]): KMeansModel = {
    transformSchema(dataset.schema, logging = true)

    val rawData: RDD[Vector] = dataset.select(col($(featuresCol))).rdd.map {
      case Row(point: Vector) => point
    }

    val instr = Instrumentation.create(this, rawData)
    instr.logParams(featuresCol, predictionCol, k, initMode, initSteps, maxIter, seed, tol)
    val featureNum = rawData.first().size
    // Compute squared norms and cache them.
    val norms = rawData.map(Vectors.norm(_, 2.0))
    norms.persist()
    val data = rawData.zip(norms).map { case (v, norm) =>
      new VectorWithNorm(v, norm)
    }

    val initContextTime = System.nanoTime()
    val sc = data.sparkContext
    PSContext.getOrCreate(sc)

    val psVector = PSVector.dense(featureNum, $(k) + 1)


    val initContextTimeInSeconds = (System.nanoTime() - initContextTime) / 1e9
    logInfo(f"Initialization context with $initMode took $initContextTimeInSeconds%.3f seconds.")

    val initStartTime = System.nanoTime()

    val centers =if ($(initMode) == KMeans.RANDOM) {
      initRandom(data, psVector)
    } else {
      initKMeansParallel(data).map { center=>
        val psCenter = PSVector.duplicate(psVector).fill(center.vector.toArray)
        new PsVectorWithNorm(psCenter.toBreeze)
      }
    }


    val initTimeInSeconds = (System.nanoTime() - initStartTime) / 1e9
    logInfo(f"Initialization with ${$(initMode)} took $initTimeInSeconds%.3f seconds.")

    var converged = false
    var cost = 0.0
    var iteration = 0

    val iterationStartTime = System.nanoTime()

    instr.logNumFeatures(featureNum)

    // Execute iterations of Lloyd's algorithm until converged
    while (iteration < $(maxIter) && !converged) {
      val costAccum = sc.doubleAccumulator
      val thisCenters = centers.map(center=>new VectorWithNorm(Vectors.dense(center.vector.pull()),center.norm))

      // Find the sum and count of points mapping to each center
      val totalContribs = data.mapPartitions { points =>

        val dims = featureNum

        val sums = Array.fill(thisCenters.length)(Vectors.zeros(dims))
        val counts = Array.fill(thisCenters.length)(0L)

        points.foreach { point =>
          val (bestCenter, cost) = KMeans.findClosest(thisCenters, point)
          costAccum.add(cost)
          val sum = sums(bestCenter)
          axpy(1.0, point.vector, sum)
          counts(bestCenter) += 1
        }

        counts.indices.filter(counts(_) > 0).map(j => (j, (sums(j), counts(j)))).iterator
      }.reduceByKey { case ((sum1, count1), (sum2, count2)) =>
        axpy(1.0, sum2, sum1)
        (sum1, count1 + count2)
      }.collectAsMap()


      // Update the cluster centers and costs
      converged = true
      totalContribs.foreach { case (j, (sum, count)) =>
        scal(1.0 / count, sum)
        val newCenter = new VectorWithNorm(sum)
        if (converged && KMeans.fastSquaredDistance(centers(j), newCenter) > $(tol) * $(tol)) {
          converged = false
        }
        centers(j).vector.push(newCenter.vector.toArray)
        centers(j) = new PsVectorWithNorm(centers(j).vector, newCenter.norm)
      }

      cost = costAccum.value
      iteration += 1
    }

    val iterationTimeInSeconds = (System.nanoTime() - iterationStartTime) / 1e9
    logInfo(f"Iterations took $iterationTimeInSeconds%.3f seconds.")

    if (iteration == $(maxIter)) {
      logInfo(s"KMeans reached the max number of iterations: $iteration.")
    } else {
      logInfo(s"KMeans converged in $iteration iterations.")
    }

    logInfo(s"The cost is $cost.")
    norms.unpersist()

    // Warn at the end of the run as well, for increased visibility.
    if (data.getStorageLevel == StorageLevel.NONE) {
      logWarning("The input data was not directly cached, which may hurt performance if its"
        + " parent RDDs are also uncached.")
    }

    val model = copyValues(new KMeansModel(uid, centers.map(center => Vectors.dense(center.vector.pull()))).setParent(this))

    centers.foreach(_.vector.delete())
    val summary = new KMeansSummary(
      model.transform(dataset), $(predictionCol), $(featuresCol), $(k))
    model.setSummary(Some(summary))
    instr.logSuccess(model)
    model
  }


  @Since("1.5.0")
  override def transformSchema(schema: StructType): StructType = {
    validateAndTransformSchema(schema)
  }
}

@Since("1.6.0")
object KMeans extends DefaultParamsReadable[KMeans] {


  // Initialization mode names
  @Since("0.8.0")
  val RANDOM = "random"
  @Since("0.8.0")
  val K_MEANS_PARALLEL = "k-means||"

  val precision=1e-6

  private[spark] def validateInitMode(initMode: String): Boolean = {
    initMode match {
      case KMeans.RANDOM => true
      case KMeans.K_MEANS_PARALLEL => true
      case _ => false
    }
  }



  /**
    * Returns the index of the closest center to the given point, as well as the squared distance.
    */
  private[ml] def findClosest(centers: TraversableOnce[VectorWithNorm],
                              point: VectorWithNorm): (Int, Double) = {
    var bestDistance = Double.PositiveInfinity
    var bestIndex = 0
    var i = 0
    centers.foreach { center =>
      // Since `\|a - b\| \geq |\|a\| - \|b\||`, we can use this lower bound to avoid unnecessary
      // distance computation.
      var lowerBoundOfSqDist = center.norm - point.norm
      lowerBoundOfSqDist = lowerBoundOfSqDist * lowerBoundOfSqDist
      if (lowerBoundOfSqDist < bestDistance) {
        val distance: Double = fastSquaredDistance(center, point)
        if (distance < bestDistance) {
          bestDistance = distance
          bestIndex = i
        }
      }
      i += 1
    }
    (bestIndex, bestDistance)
  }


  /**
    * Returns the index of the closest center to the given point, as well as the squared distance.
    */
  private[ml] def findClosestLocal(centers: TraversableOnce[VectorWithNorm],
                              point: VectorWithNorm): (Int, Double) = {
    var bestDistance = Double.PositiveInfinity
    var bestIndex = 0
    var i = 0
    centers.foreach { center =>
      // Since `\|a - b\| \geq |\|a\| - \|b\||`, we can use this lower bound to avoid unnecessary
      // distance computation.
      var lowerBoundOfSqDist = center.norm - point.norm
      lowerBoundOfSqDist = lowerBoundOfSqDist * lowerBoundOfSqDist
      if (lowerBoundOfSqDist < bestDistance) {
        val distance: Double = fastSquaredDistance(center, point)
        if (distance < bestDistance) {
          bestDistance = distance
          bestIndex = i
        }
      }
      i += 1
    }
    (bestIndex, bestDistance)
  }




  /**
    * Returns the K-means cost of a given point against the given cluster centers.
    */
  private[ml] def pointCost(centers: TraversableOnce[VectorWithNorm],
                            point: VectorWithNorm): Double =
    findClosest(centers, point)._2

  /**
    * Returns the squared Euclidean distance between two vectors
    */
  private[clustering] def fastSquaredDistance(
                                               v1: Vector,
                                               norm1: Double,
                                               v2: Vector,
                                               norm2: Double): Double = {
    val n = v1.size
    require(v2.size == n)
    require(norm1 >= 0.0 && norm2 >= 0.0)
    val sumSquaredNorm = norm1 * norm1 + norm2 * norm2
    val normDiff = norm1 - norm2
    var sqDist = 0.0
    lazy val EPSILON = {
      var eps = 1.0
      while ((1.0 + (eps / 2.0)) != 1.0) {
        eps /= 2.0
      }
      eps
    }
    /*
     * The relative error is
     * <pre>
     * EPSILON * ( \|a\|_2^2 + \|b\\_2^2 + 2 |a^T b|) / ( \|a - b\|_2^2 ),
     * </pre>
     * which is bounded by
     * <pre>
     * 2.0 * EPSILON * ( \|a\|_2^2 + \|b\|_2^2 ) / ( (\|a\|_2 - \|b\|_2)^2 ).
     * </pre>
     * The bound doesn't need the inner product, so we can use it as a sufficient condition to
     * check quickly whether the inner product approach is accurate.
     */
    val precisionBound1 = 2.0 * EPSILON * sumSquaredNorm / (normDiff * normDiff + EPSILON)
    if (precisionBound1 < precision) {
      sqDist = sumSquaredNorm - 2.0 * dot(v1, v2)
    } else if (v1.isInstanceOf[SparseVector] || v2.isInstanceOf[SparseVector]) {
      val dotValue = dot(v1, v2)
      sqDist = math.max(sumSquaredNorm - 2.0 * dotValue, 0.0)
      val precisionBound2 = EPSILON * (sumSquaredNorm + 2.0 * math.abs(dotValue)) /
        (sqDist + EPSILON)
      if (precisionBound2 > precision) {
        sqDist = Vectors.sqdist(v1, v2)
      }
    } else {
      sqDist = Vectors.sqdist(v1, v2)
    }
    sqDist
  }

  private[clustering] def fastSquaredDistance(center: PsVectorWithNorm,
                                              point: VectorWithNorm): Double = {
    fastSquaredDistance(Vectors.dense(center.vector.pull()), center.norm, point.vector, point.norm)
  }


  private[clustering] def fastSquaredDistance(center: VectorWithNorm,
                                              point: VectorWithNorm): Double = {
    fastSquaredDistance(center.vector, center.norm, point.vector, point.norm)
  }


  private[clustering] def fastSquaredDistance(center1: PsVectorWithNorm,
                                              center2: PsVectorWithNorm): Double = {
    fastSquaredDistance(center1,new VectorWithNorm(Vectors.dense(center2.vector.pull()),center2.norm))
  }


  @Since("1.6.0")
  override def load(path: String): KMeans = super.load(path)
}

/**
  * :: Experimental ::
  * Summary of KMeans.
  *
  * @param predictions   `DataFrame` produced by `KMeansModel.transform()`.
  * @param predictionCol Name for column of predicted clusters in `predictions`.
  * @param featuresCol   Name for column of features in `predictions`.
  * @param k             Number of clusters.
  */
@Since("2.0.0")
@Experimental
class KMeansSummary private[clustering](
                                         predictions: DataFrame,
                                         predictionCol: String,
                                         featuresCol: String,
                                         k: Int) extends ClusteringSummary(predictions, predictionCol, featuresCol, k)


/**
  * A vector with its norm for fast distance computation.
  *
  * @see [[KMeans#fastSquaredDistance]]
  */
private[clustering]
class VectorWithNorm(val vector: Vector, val norm: Double) extends Serializable {

  def this(vector: Vector) = this(vector, Vectors.norm(vector, 2.0))

  def this(array: Array[Double]) = this(Vectors.dense(array))

  /** Converts the vector to a dense vector. */
  def toDense: VectorWithNorm = new VectorWithNorm(Vectors.dense(vector.toArray), norm)
}

/**
  * A vector with its norm for fast distance computation.
  *
  * @see [[KMeans#fastSquaredDistance]]
  */
private[clustering]
class PsVectorWithNorm(val vector: BreezePSVector, val norm: Double) extends Serializable {
  def this(vector: BreezePSVector) = this(vector, vector.norm(2))
}


/**
  * Model fitted by KMeans.
  *
  */
@Since("1.5.0")
class KMeansModel private[ml](
                               @Since("1.5.0") override val uid: String,
                               val clusterCenters: Array[Vector])
  extends Model[KMeansModel] with KMeansParams with MLWritable {

  @Since("1.5.0")
  override def copy(extra: ParamMap): KMeansModel = {
    val copied = copyValues(new KMeansModel(uid, clusterCenters), extra)
    copied.setSummary(trainingSummary).setParent(this.parent)
  }

  /** @group setParam */
  @Since("2.0.0")
  def setFeaturesCol(value: String): this.type = set(featuresCol, value)

  /** @group setParam */
  @Since("2.0.0")
  def setPredictionCol(value: String): this.type = set(predictionCol, value)

  private val clusterCentersWithNorm = {
    if (clusterCenters == null || clusterCenters.length == 0) null
    else {
      val featureNum = clusterCenters(0).size
      clusterCenters.map(v => new VectorWithNorm(v))
    }
  }


  @Since("2.0.0")
  override def transform(dataset: Dataset[_]): DataFrame = {
    transformSchema(dataset.schema, logging = true)
    val predictUDF = udf((vector: Vector) => predict(vector))
    dataset.withColumn($(predictionCol), predictUDF(col($(featuresCol))))
  }

  @Since("1.5.0")
  override def transformSchema(schema: StructType): StructType = {
    validateAndTransformSchema(schema)
  }

  private[clustering] def predict(features: Vector): Int = {
    KMeans.findClosest(clusterCentersWithNorm, new VectorWithNorm(features))._1
  }


  /**
    * Return the K-means cost (sum of squared distances of points to their nearest center) for this
    * model on the given data.
    */
  // TODO: Replace the temp fix when we have proper evaluators defined for clustering.
  @Since("2.0.0")
  def computeCost(dataset: Dataset[_]): Double = {
    SchemaUtils.checkColumnType(dataset.schema, $(featuresCol), new VectorUDT)
    val data: RDD[OldVector] = dataset.select(col($(featuresCol))).rdd.map {
      case Row(point: Vector) => OldVectors.fromML(point)
    }
    val cost = data
      .map(p => KMeans.pointCost(clusterCentersWithNorm, new VectorWithNorm(p))).sum()
    cost
  }

  /**
    * Returns a [[org.apache.spark.ml.util.MLWriter]] instance for this ML instance.
    *
    * For [[KMeansModel]], this does NOT currently save the training [[summary]].
    * An option to save [[summary]] may be added in the future.
    *
    */
  @Since("1.6.0")
  override def write: MLWriter = new KMeansModel.KMeansModelWriter(this)

  private var trainingSummary: Option[KMeansSummary] = None

  private[clustering] def setSummary(summary: Option[KMeansSummary]): this.type = {
    this.trainingSummary = summary
    this
  }

  /**
    * Return true if there exists summary of model.
    */
  @Since("2.0.0")
  def hasSummary: Boolean = trainingSummary.nonEmpty

  /**
    * Gets summary of model on training set. An exception is
    * thrown if `trainingSummary == None`.
    */
  @Since("2.0.0")
  def summary: KMeansSummary = trainingSummary.getOrElse {
    throw new SparkException(
      s"No training summary available for the ${this.getClass.getSimpleName}")
  }
}

@Since("1.6.0")
object KMeansModel extends MLReadable[KMeansModel] {

  @Since("1.6.0")
  override def read: MLReader[KMeansModel] = new KMeansModelReader

  @Since("1.6.0")
  override def load(path: String): KMeansModel = super.load(path)

  /** Helper class for storing model data */
  private case class Data(clusterIdx: Int, clusterCenter: Vector)

  /**
    * We store all cluster centers in a single row and use this class to store model data by
    * Spark 1.6 and earlier. A model can be loaded from such older data for backward compatibility.
    */
  private case class OldData(clusterCenters: Array[OldVector])

  /** [[MLWriter]] instance for [[KMeansModel]] */
  private[KMeansModel] class KMeansModelWriter(instance: KMeansModel) extends MLWriter {

    override protected def saveImpl(path: String): Unit = {
      // Save metadata and Params
      DefaultParamsWriter.saveMetadata(instance, path, sc)
      // Save model data: cluster centers
      val data: Array[Data] = instance.clusterCenters.zipWithIndex.map { case (center, idx) =>
        Data(idx, center)
      }
      val dataPath = new Path(path, "data").toString
      sparkSession.createDataFrame(data).repartition(1).write.parquet(dataPath)
    }
  }

  private class KMeansModelReader extends MLReader[KMeansModel] {

    /** Checked against metadata when loading model */
    private val className = classOf[KMeansModel].getName

    override def load(path: String): KMeansModel = {
      // Import implicits for Dataset Encoder
      val sparkSession = super.sparkSession
      import sparkSession.implicits._

      val metadata = DefaultParamsReader.loadMetadata(path, sc, className)
      val dataPath = new Path(path, "data").toString

      val clusterCenters = if (majorVersion(metadata.sparkVersion) >= 2) {
        val data: Dataset[Data] = sparkSession.read.parquet(dataPath).as[Data]
        data.collect().sortBy(_.clusterIdx).map(_.clusterCenter)
      } else {
        // Loads KMeansModel stored with the old format used by Spark 1.6 and earlier.
        sparkSession.read.parquet(dataPath).as[OldData].head().clusterCenters.map(center => Vectors.fromBreeze(center.asBreeze))
      }
      val model = new KMeansModel(metadata.uid, clusterCenters)
      DefaultParamsReader.getAndSetParams(model, metadata)
      model
    }
  }

}
