package com.tencent.angel.spark.ml.featureEngineering.Dummy

import com.tencent.angel.spark.ml.util.LogUtils
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel

/**
 * Created by isakjiang on 2020-11-16
 */
object OneHotEncoder {

  def hashCode(str: String): Long = {
    var hash: Long = 0
    str.foreach(c => hash = 31 * hash + c)
    hash
  }

  case class Instance(target: String, features: Array[String])

  val topFeatureThreshold = 10000

  def processOneHot(dataSet: RDD[(String, Array[String])],
                    countThreshold: Int,
                    baseFeatIndexRDD: RDD[(String, Int)] = null): (RDD[(String, Int)], RDD[(String, Array[Int])]) = {

    val (featIndexCountRDD, featIndexRDD, finalFeatIndexRDD) = if (baseFeatIndexRDD != null) {
      // baseFeatIndexRDD is not null, it means that processOneHot without warm start.
      val featCountRDD = dataSet.flatMap { case (target, feat) => feat.map(f => (f, 1)) }
        .reduceByKey(_ + _)

      baseFeatIndexRDD.cache()
      val baseFeatCount = baseFeatIndexRDD.count().toInt
      LogUtils.logTime(s"base feature count: $baseFeatCount")

      val extraFeatIndexRDD = featCountRDD.subtractByKey(baseFeatIndexRDD)
        .filter { case (featName, count) => count >= countThreshold }
        .sortBy(x => x._1)
        .zipWithIndex()
        .map { case ((featName, count), index) =>
          (featName, index.toInt + baseFeatCount)
        }

      val featIndex = featCountRDD.join(baseFeatIndexRDD)
        .map { case (featName, (count, index)) => (featName, index) }
        .union(extraFeatIndexRDD)
      featIndex.cache()

      val finalFeatIndex = baseFeatIndexRDD.union(featIndex).distinct()
        .sortBy(x => x._2)

      val featCountIndex = featCountRDD.join(featIndex)
        .map { case (featName, (count, index)) => (featName, count, index) }

      (featCountIndex, featIndex, finalFeatIndex)
    } else {
      // baseFeatIndexRDD is null
      val featCountRDD = dataSet.flatMap { case (target, feat) => feat.map(f => (f, 1)) }
        .reduceByKey(_ + _)
        .filter { case (featName, count) => count >= countThreshold }
      val featCountIndex = featCountRDD.sortBy(x => x._1)
        .zipWithIndex()
        .map { case ((featName, count), index) => (featName, count, index.toInt) }

      val featIndex = featCountIndex.map { case (featName, count, index) => (featName, index) }

      (featCountIndex, featIndex, featIndex)
    }

    featIndexRDD.cache()
    featIndexCountRDD.cache()
    LogUtils.logTime(s"feature count: ${featIndexRDD.count()}")
    LogUtils.logTime(s"total feature count:${finalFeatIndexRDD.count()}")

    val instances = dataSet.zipWithUniqueId().persist(StorageLevel.DISK_ONLY)
    //    instances.checkpoint()

    LogUtils.logTime(s"instances count: ${instances.count()}")

    val instance2Target = instances.map { case ((target, feat), instId) =>
      (instId, target)
    }
    instance2Target.cache()
    instance2Target.count()

    val feat2InstIdsRDD = instances.flatMap { case ((target, feat), instId) =>
      feat.map(featName => Tuple2(featName, instId))
    }.groupByKey()

    feat2InstIdsRDD.persist(StorageLevel.DISK_ONLY)
    feat2InstIdsRDD.count()
    instances.unpersist()

    val slidingLen = 100000
    val instance2FeatRDD = feat2InstIdsRDD.join(featIndexRDD)
      .flatMap { case (featName, (instIds, featId)) =>
        instIds.sliding(slidingLen, slidingLen)
          .map { slice => (slice, featId) }
      }.flatMap { case (slice, featId) =>
      slice.map(instId => (instId, featId))
    }.groupByKey()

    LogUtils.logTime(s"instance2FeatRDD count: ${instance2FeatRDD.count()} " +
      s"instance2Target count: ${instance2Target.count()}")

    val oneHotInstances = instance2FeatRDD.join(instance2Target)
      .map { case (id, (feat, target)) =>
        (target, feat.toArray.sortWith(_ < _))
      }

    LogUtils.logTime(s"oneHotInstances count: ${oneHotInstances.count()}")

    (finalFeatIndexRDD, oneHotInstances)
  }
}
