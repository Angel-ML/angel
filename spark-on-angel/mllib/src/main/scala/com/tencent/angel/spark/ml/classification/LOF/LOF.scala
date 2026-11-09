package org.apache.spark.ml.outlier

import org.apache.spark.ml.Transformer
import org.apache.spark.ml.linalg.{Vector, VectorUDT, Vectors}
import org.apache.spark.ml.param.shared.{HasFeaturesCol, HasOutputCol}
import org.apache.spark.ml.param._
import org.apache.spark.ml.util.{Identifiable, SchemaUtils}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{DataTypes, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Dataset, Row}

import scala.collection.mutable.ArrayBuffer

private[ml] trait LOFParams extends Params
  with HasFeaturesCol with HasOutputCol {

  /**
    * The minimum number of points.
    */
  final val minPts = new IntParam(
    this, "minPts", "a minimum number of points"
  )
  setDefault(minPts -> 5)

  final val distType = new Param[String](
    this, "distType", "the type of distance",
    isValid = ParamValidators.inArray(Array(LOF.euclidean))
  )
  setDefault(distType -> LOF.euclidean)
}

class LOF(override val uid: String) extends Transformer with LOFParams {

  def this() = this(Identifiable.randomUID("lof"))

  /**
    * Sets the value of param [[minPts]].
    * Default is 5.
    */
  def setMinPts(value: Int): this.type = {
    require(value > 0, "minPts must be a positive integer.")
    set(minPts, value)
  }

  def setFeaturesCol(value: String): this.type = set(featuresCol, value)

  override def transform(dataset: Dataset[_]): DataFrame = {
    val input = dataset.select(col($(featuresCol))).rdd.map {
      case Row(vec: Vector) => vec
    }
    val session = dataset.sparkSession
    val sc = session.sparkContext
    val indexedPointsRDD = input.zipWithIndex().map(_.swap).persist()
    val numPartitionsOfIndexedPointsRDD = indexedPointsRDD.getNumPartitions

    // compute k-distance neighborhood of each point
    val neighborhoodRDD = Range(0, numPartitionsOfIndexedPointsRDD).map { outId: Int =>
      val outPart = indexedPointsRDD.mapPartitionsWithIndex { (inId, iter) =>
        if (inId == outId) {
          iter
        } else {
          Iterator.empty
        }
      }.collect()
      val bcOutPart = sc.broadcast(outPart)
      indexedPointsRDD.mapPartitions { inPart =>
        val part = inPart.toArray
        val buf = new ArrayBuffer[(Long, Array[(Long, Double)])]()
        bcOutPart.value.foreach { case (idx: Long, vec: Vector) =>
          val neighborhood = computeKDistanceNeighborhood(part, vec, $(minPts), $(distType))
          buf.append((idx, neighborhood))
        }
        buf.iterator
      }.reduceByKey(combineNeighborhood)
    }.reduce(_.union(_))

    val swappedRDD = neighborhoodRDD.flatMap {
      case (outIdx: Long, neighborhood: Array[(Long, Double)]) =>
        neighborhood.map { case (inIdx: Long, dist: Double) =>
          (inIdx, (outIdx, dist))
        } :+ (outIdx, (outIdx, 0d))
    }.groupByKey().persist()

    val localOutlierFactorRDD = swappedRDD.cogroup(neighborhoodRDD)
      .flatMap { case (outIdx: Long,
      (k: Iterable[Iterable[(Long, Double)]], v: Iterable[Array[(Long, Double)]])) =>
        require(k.size == 1 && v.size == 1)
        val kDistance = v.head.last._2
        k.head.filter(_._1 != outIdx).map { case (inIdx: Long, dist: Double) =>
          (inIdx, (outIdx, Math.max(dist, kDistance)))
        }
      }.groupByKey().map { case (idx: Long, iter: Iterable[(Long, Double)]) =>
      val num = iter.size
      val sum = iter.map(_._2).sum
      (idx, num / sum)
    }.cogroup(swappedRDD).flatMap {
      case (outIdx: Long, (k: Iterable[Double], v: Iterable[Iterable[(Long, Double)]])) =>
        require(k.size == 1 && v.size == 1)
        val lrd = k.head
        v.head.map { case (inIdx: Long, dist: Double) =>
          (inIdx, (outIdx, lrd))
        }
    }.groupByKey().map { case (idx: Long, iter: Iterable[(Long, Double)]) =>
      require(iter.exists(_._1 == idx))
      val lrd = iter.find(_._1 == idx).get._2
      val sum = iter.filter(_._1 != idx).map(_._2).sum
      (idx, sum / lrd / (iter.size - 1))
    }

    val finalRDD = localOutlierFactorRDD.join(indexedPointsRDD)
      .map(r => Row(r._1, r._2._1, r._2._2))
    val schema = transformSchema(dataset.schema)
    session.createDataFrame(finalRDD, schema)
  }

  override def copy(extra: ParamMap): Transformer = defaultCopy(extra)

  override def transformSchema(schema: StructType): StructType = {
    SchemaUtils.checkColumnType(schema, $(featuresCol), new VectorUDT())

    getOutputSchema
  }

  def getOutputSchema: StructType = {
    StructType(Array(
      StructField(LOF.index, DataTypes.LongType, nullable = false),
      StructField(LOF.lof, DataTypes.DoubleType, nullable = false),
      StructField($(featuresCol), new VectorUDT(), nullable = false)
    ))
  }

  /**
    * Compute the k-distance neighborhood.
    *
    * @param data
    * @param target
    * @param k
    * @param distType
    * @return
    */
  def computeKDistanceNeighborhood(data: Array[(Long, Vector)],
                                   target: Vector,
                                   k: Int,
                                   distType: String): Array[(Long, Double)] = {
    /**
      * Move elements in data from the start position to right by one unit.
      *
      * @param data
      * @param start
      * @return
      */
    def moveRight(data: ArrayBuffer[(Long, Double)], start: Int): ArrayBuffer[(Long, Double)] = {
      require(start >= 0 && start < data.length)
      if (data.nonEmpty) {
        data.append(data.last)
        var i = data.length - 1
        while (i > start) {
          data(i) = data(i - 1)
          i -= 1
        }
      }
      data
    }

    var idxAndDist = new ArrayBuffer[(Long, Double)]()
    var count = 0 // the size of distinct instances
    data.foreach { case (idx: Long, vec: Vector) =>
      val targetDist = distType match {
        case LOF.euclidean =>
          Vectors.sqdist(target, vec)
      }
      // targetDist equals zero when computing distance between vec and itself
      if (targetDist > 0d) {
        var i = 0
        var inserted = false
        while (i < idxAndDist.length && !inserted) {
          val dist = idxAndDist(i)._2
          if (targetDist <= dist) {
            if (count < k) {
              idxAndDist = moveRight(idxAndDist, i)
              idxAndDist(i) = (idx, targetDist)
              if (targetDist < dist) {
                count += 1
              }
            } else if (count == k) {
              if (targetDist == dist) {
                idxAndDist = moveRight(idxAndDist, i)
                idxAndDist(i) = (idx, targetDist)
              } else {
                var sameRight = 0
                var j = idxAndDist.length - 1
                while (j > 0 && idxAndDist(j)._2 == idxAndDist(j - 1)._2) {
                  sameRight += 1
                  j -= 1
                }
                if (dist == idxAndDist.last._2) {
                  idxAndDist = idxAndDist.dropRight(sameRight)
                } else {
                  idxAndDist = idxAndDist.dropRight(sameRight + 1)
                  idxAndDist = moveRight(idxAndDist, i)
                }
                idxAndDist(i) = (idx, targetDist)
              }
            } else {
              throw new Exception(s"count($count) should not larger than k($k)")
            }
            inserted = true
          }
          i += 1
        }
        if (count < k && !inserted) {
          idxAndDist.append((idx, targetDist))
          count += 1
        }
      }
    }
    idxAndDist.toArray
  }

  /**
    * Combine neighborhood between two partitions. The time complexity of this function is O(m + n).
    *
    * @param first
    * @param second
    * @return
    */
  def combineNeighborhood(first: Array[(Long, Double)],
                          second: Array[(Long, Double)]): Array[(Long, Double)] = {

    var pos1 = 0
    var pos2 = 0
    var count = 0 // the size of distinct instances
    val combined = new ArrayBuffer[(Long, Double)]()

    while (pos1 < first.length && pos2 < second.length && count < $(minPts)) {
      if (first(pos1)._2 == second(pos2)._2) {
        combined.append(first(pos1))
        pos1 += 1
        if (combined.length == 1) {
          count += 1
        } else {
          if (combined(combined.length - 1) != combined(combined.length - 2)) {
            count += 1
          }
        }
        combined.append(second(pos2))
        pos2 += 1
      } else {
        if (first(pos1)._2 < second(pos2)._2) {
          combined.append(first(pos1))
          pos1 += 1
        } else {
          combined.append(second(pos2))
          pos2 += 1
        }
        if (combined.length == 1) {
          count += 1
        } else {
          if (combined(combined.length - 1) != combined(combined.length - 2)) {
            count += 1
          }
        }
      }
    }

    while (pos1 < first.length && count < $(minPts)) {
      combined.append(first(pos1))
      pos1 += 1
      if (combined.length == 1) {
        count += 1
      } else {
        if (combined(combined.length - 1) != combined(combined.length - 2)) {
          count += 1
        }
      }
    }

    while (pos2 < second.length && count < $(minPts)) {
      combined.append(second(pos2))
      pos2 += 1
      if (combined.length == 1) {
        count += 1
      } else {
        if (combined(combined.length - 1) != combined(combined.length - 2)) {
          count += 1
        }
      }
    }

    combined.toArray
  }

}

object LOF {
  /** String name for "euclidean" (euclidean distance). */
  private[ml] val euclidean = "euclidean"

  /** Set of types of distances that Local Outlier Factor supports. */
  private[ml] val supportedDistTypes = Array(euclidean)

  /** String name for column "index" */
  private[ml] val index = "index"

  /** String name for column "lof" */
  private[ml] val lof = "lof"

  /** String name for column "vector" */
//  private[ml] val features = "features"
}
