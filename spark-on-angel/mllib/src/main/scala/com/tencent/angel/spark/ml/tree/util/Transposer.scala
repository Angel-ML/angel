/*
 * Tencent is pleased to support the open source community by making Angel available.
 *
 * Copyright (C) 2017-2018 THL A29 Limited, a Tencent company. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in
 * compliance with the License. You may obtain a copy of the License at
 *
 * https://opensource.org/licenses/Apache-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 *
 */

package com.tencent.angel.spark.ml.tree.util

import com.tencent.angel.spark.ml.tree.common.TreeConf._
import com.tencent.angel.spark.ml.tree.data.{FeatureRow, Instance, InstanceRow}
import com.tencent.angel.spark.ml.tree.gbdt.GBDTTrainer
import com.tencent.angel.spark.ml.tree.gbdt.metadata.FeatureInfo
import com.tencent.angel.spark.ml.tree.sketch.HeapQuantileSketch
import com.tencent.angel.spark.ml.tree.param.GBDTParam
import it.unimi.dsi.fastutil.bytes.ByteArrayList
import it.unimi.dsi.fastutil.doubles.DoubleArrayList
import it.unimi.dsi.fastutil.ints.IntArrayList
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.ArrayBuffer

object Transposer {
  private val param = new GBDTParam

  type FR = (Array[Int], Array[Double])

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("GBDT")
    implicit val sc = SparkContext.getOrCreate(conf)

    param.numClass = conf.getInt(ML_NUM_CLASS, DEFAULT_ML_NUM_CLASS)
    param.numFeature = conf.get(ML_NUM_FEATURE).toInt
    param.featSampleRatio = conf.getDouble(ML_FEATURE_SAMPLE_RATIO, DEFAULT_ML_FEATURE_SAMPLE_RATIO).toFloat
    param.numWorker = conf.get(ML_NUM_WORKER).toInt
    param.numThread = conf.getInt(ML_NUM_THREAD, DEFAULT_ML_NUM_THREAD)
    param.lossFunc = conf.get(ML_LOSS_FUNCTION)
    param.evalMetrics = conf.get(ML_EVAL_METRIC, DEFAULT_ML_EVAL_METRIC).split(",").map(_.trim).filter(_.nonEmpty)
    param.learningRate = conf.getDouble(ML_LEARN_RATE, DEFAULT_ML_LEARN_RATE).toFloat
    param.histSubtraction = conf.getBoolean(ML_GBDT_HIST_SUBTRACTION, DEFAULT_ML_GBDT_HIST_SUBTRACTION)
    param.lighterChildFirst = conf.getBoolean(ML_GBDT_LIGHTER_CHILD_FIRST, DEFAULT_ML_GBDT_LIGHTER_CHILD_FIRST)
    param.fullHessian = conf.getBoolean(ML_GBDT_FULL_HESSIAN, DEFAULT_ML_GBDT_FULL_HESSIAN)
    param.numSplit = conf.getInt(ML_GBDT_SPLIT_NUM, DEFAULT_ML_GBDT_SPLIT_NUM)
    param.numTree = conf.getInt(ML_GBDT_TREE_NUM, DEFAULT_ML_GBDT_TREE_NUM)
    param.maxDepth = conf.getInt(ML_GBDT_MAX_DEPTH, DEFAULT_ML_GBDT_MAX_DEPTH)
    val maxNodeNum = Maths.pow(2, param.maxDepth + 1) - 1
    param.maxNodeNum = conf.getInt(ML_GBDT_MAX_NODE_NUM, maxNodeNum) min maxNodeNum
    param.minChildWeight = conf.getDouble(ML_GBDT_MIN_CHILD_WEIGHT, DEFAULT_ML_GBDT_MIN_CHILD_WEIGHT).toFloat
    param.minSplitGain = conf.getDouble(ML_GBDT_MIN_SPLIT_GAIN, DEFAULT_ML_GBDT_MIN_SPLIT_GAIN).toFloat
    param.regAlpha = conf.getDouble(ML_GBDT_REG_ALPHA, DEFAULT_ML_GBDT_REG_ALPHA).toFloat
    param.regLambda = conf.getDouble(ML_GBDT_REG_LAMBDA, DEFAULT_ML_GBDT_REG_LAMBDA).toFloat max 1.0f
    param.maxLeafWeight = conf.getDouble(ML_GBDT_MAX_LEAF_WEIGHT, DEFAULT_ML_GBDT_MAX_LEAF_WEIGHT).toFloat

    val input = conf.get(ML_TRAIN_PATH)
    val validRatio = conf.getDouble(ML_VALID_DATA_RATIO, DEFAULT_ML_VALID_DATA_RATIO)
    val oriTrainData = loadData(input, validRatio)

    val numKV = oriTrainData.map(ins => ins.feature.numActives).reduce(_ + _)

    val res = transpose(oriTrainData)

    val numKV2 = res._1.map {
      case Some(row) => row.size
      case None => 0
    }.reduce(_ + _)
    println(s"#KV pairs: $numKV vs. $numKV2")
  }

  def loadData(input: String,
               validRatio: Double)(implicit sc: SparkContext): RDD[Instance] = {
    val loadStart = System.currentTimeMillis()
    // 1. load original data, split into train data and valid data
    val dim = param.numFeature
    val fullDataset = sc.textFile(input)
      .map(_.trim)
      .filter(_.nonEmpty)
      .map(line => DataLoader.parseLibsvm(line, dim))
      .persist(StorageLevel.MEMORY_AND_DISK)
    val dataset = fullDataset.randomSplit(Array[Double](1.0 - validRatio, validRatio))
    val trainData = dataset(0).persist(StorageLevel.MEMORY_AND_DISK)
    val validData = dataset(1).persist(StorageLevel.MEMORY_AND_DISK)
    val numTranData = trainData.count().toInt
    val numValidData = validData.count().toInt
    fullDataset.unpersist()
    println(s"Load data cost ${System.currentTimeMillis() - loadStart} ms, " +
      s"$numTranData train data, $numValidData valid data")
    trainData
  }

  def transpose(dpData: RDD[Instance])(implicit sc: SparkContext): (RDD[Option[FeatureRow]], Broadcast[FeatureInfo], Broadcast[Array[Float]]) = {
    val transposeStart = System.currentTimeMillis()
    val numFeature = param.numFeature

    val oriNumPart = dpData.getNumPartitions
    val partNumInstance = new Array[Int](oriNumPart)
    dpData.mapPartitionsWithIndex((partId, iterator) =>
      Seq((partId, iterator.size)).iterator)
      .collect()
      .foreach(part => partNumInstance(part._1) = part._2)
    val partInsIdOffset = new Array[Int](oriNumPart)
    for (i <- 1 until oriNumPart)
      partInsIdOffset(i) += partInsIdOffset(i - 1) + partNumInstance(i - 1)
    val bcNumTrainData = sc.broadcast(partNumInstance.sum)
    val bcPartInsIdOffset = sc.broadcast(partInsIdOffset)

    // labels
    val labelsRdd = dpData.mapPartitionsWithIndex((partId, iterator) => {
      val offsets = bcPartInsIdOffset.value
      val partSize = if (partId + 1 == offsets.length) {
        bcNumTrainData.value - offsets(partId)
      } else {
        offsets(partId + 1) - offsets(partId)
      }
      val labels = new Array[Float](partSize)
      var count = 0
      while (iterator.hasNext) {
        labels(count) = iterator.next().label.toFloat
        count += 1
      }
      require(count == partSize)
      Seq((offsets(partId), labels)).iterator
    }, preservesPartitioning = true)

    val labels = new Array[Float](bcNumTrainData.value)
    labelsRdd.collect().foreach(part => {
      val offset = part._1
      val partLabels = part._2
      for (i <- partLabels.indices)
        labels(offset + i) = partLabels(i)
    })
    dpData.unpersist()
    GBDTTrainer.ensureLabel(labels, param.numClass)

    val bcLabels = sc.broadcast(labels)
    println(s"Collect labels cost ${System.currentTimeMillis() - transposeStart} ms")

    // 1. transpose to FP dataset
    val toFPStart = System.currentTimeMillis()
    val evenPartitioner = new EvenPartitioner(param.numFeature, param.numWorker)
    val bcFeatureEdges = sc.broadcast(evenPartitioner.partitionEdges())
    val fpData = dpData.mapPartitionsWithIndex((partId, iterator) => {
      val startTime = System.currentTimeMillis()
      val insIdLists = new Array[IntArrayList](numFeature)
      val valueLists = new Array[DoubleArrayList](numFeature)
      for (fid <- 0 until numFeature) {
        insIdLists(fid) = new IntArrayList()
        valueLists(fid) = new DoubleArrayList()
      }
      var insId = bcPartInsIdOffset.value(partId)
      while (iterator.hasNext) {
        iterator.next().feature.foreachActive((fid, value) => {
          insIdLists(fid).add(insId)
          valueLists(fid).add(value)
        })
        insId += 1
      }
      val featRows = new Array[(Int, FR)](numFeature)
      for (fid <- 0 until numFeature) {
        if (insIdLists(fid).size() > 0) {
          val indices = insIdLists(fid).toIntArray(null)
          val values = valueLists(fid).toDoubleArray(null)
          featRows(fid) = (fid, (indices, values))
        } else {
          featRows(fid) = (fid, null)
        }
      }
      println(s"Local transpose cost ${System.currentTimeMillis() - startTime} ms")
      featRows.iterator
    }).repartitionAndSortWithinPartitions(evenPartitioner)
      .mapPartitionsWithIndex((partId, iterator) => {
        val startTime = System.currentTimeMillis()
        val featLo = bcFeatureEdges.value(partId)
        val featHi = bcFeatureEdges.value(partId + 1)
        val featureRows = new ArrayBuffer[Option[FR]](featHi - featLo)
        val partFeatRows = new collection.mutable.ArrayBuffer[FR]()
        var curFid = featLo
        while (iterator.hasNext) {
          val entry = iterator.next()
          val fid = entry._1
          val partRow = entry._2
          require(featLo <= fid && fid < featHi)
          if (fid != curFid) {
            featureRows += compact(partFeatRows)
            partFeatRows.clear()
            curFid = fid
            partFeatRows += partRow
          } else if (!iterator.hasNext) {
            partFeatRows += partRow
            featureRows += compact(partFeatRows)
          } else {
            partFeatRows += partRow
          }
        }
        println(s"Merge feature rows cost ${System.currentTimeMillis() - startTime} ms")
        featureRows.iterator
      }).persist(StorageLevel.MEMORY_AND_DISK)
    require(fpData.count() == numFeature)
    println(s"To FP cost ${System.currentTimeMillis() - toFPStart} ms")
    // 2. splits
    val getSplitStart = System.currentTimeMillis()
    val bcParam = sc.broadcast(param)
    val splits = new Array[Array[Float]](numFeature)
    fpData.mapPartitionsWithIndex((partId, iterator) => {
      val startTime = System.currentTimeMillis()
      var curFid = bcFeatureEdges.value(partId)
      val numSplit = bcParam.value.numSplit
      val splits = collection.mutable.ArrayBuffer[(Int, Array[Float])]()
      while (iterator.hasNext) {
        iterator.next() match {
          case Some(row) => {
            val sketch = new HeapQuantileSketch()
            for (v <- row._2)
              sketch.update(v.toFloat)
            splits += ((curFid, sketch.getQuantiles(numSplit)))
          }
          case None =>
        }
        curFid += 1
      }
      println(s"Create sketch and get split cost ${System.currentTimeMillis() - startTime} ms")
      splits.iterator
    }).collect()
      .foreach(s => splits(s._1) = s._2)
    val bcFeatureInfo = sc.broadcast(FeatureInfo(numFeature, splits))
    println(s"Get split cost ${System.currentTimeMillis() - getSplitStart} ms")
    // 3. truncate
    val truncateStart = System.currentTimeMillis()
    val res = fpData.mapPartitionsWithIndex((partId, iterator) => {
      val startTime = System.currentTimeMillis()
      val featLo = bcFeatureEdges.value(partId)
      val featHi = bcFeatureEdges.value(partId + 1)
      var curFid = featLo
      val featureRows = new Array[Option[FeatureRow]](featHi - featLo)
      val splits = bcFeatureInfo.value.splits
      while (iterator.hasNext) {
        featureRows(curFid - featLo) = iterator.next() match {
          case Some(row) => {
            val indices = row._1
            val bins = row._2.map(v => Maths.indexOf(splits(curFid), v.toFloat))
            Option(FeatureRow(indices, bins))
          }
          case None => Option.empty
        }
        curFid += 1
      }
      require(curFid == featHi, s"cur fid = $curFid, should be $featHi")
      println(s"Truncate cost ${System.currentTimeMillis() - startTime} ms")
      featureRows.iterator
    }).persist(StorageLevel.MEMORY_AND_DISK)
    println(s"Truncate cost ${System.currentTimeMillis() - truncateStart} ms")
    fpData.unpersist()

    println(s"Transpose train data cost ${System.currentTimeMillis() - transposeStart} ms, " +
      s"feature edges: [${bcFeatureEdges.value.mkString(", ")}]")
    (res, bcFeatureInfo, bcLabels)
  }

  def compact(rows: Seq[FR]): Option[FR] = {
    val nonEmptyRows = rows.filter(r => r != null && r._1 != null && r._1.length != 0)
    if (nonEmptyRows.isEmpty) {
      Option.empty
    } else {
      val size = nonEmptyRows.map(_._1.length).sum
      val indices = new Array[Int](size)
      val values = new Array[Double](size)
      var offset = 0
      nonEmptyRows.sortWith((row1, row2) => row1._1(0) < row2._1(0))
        .foreach(row => {
          val partSize = row._1.length
          Array.copy(row._1, 0, indices, offset, partSize)
          Array.copy(row._2, 0, values, offset, partSize)
          offset += partSize
        })
      Option((indices, values))
    }
  }

}


class Transposer {

  @transient implicit val sc = SparkContext.getOrCreate()

  def transpose2(oriData: RDD[Instance],
                 numFeature: Int,
                 numWorker: Int,
                 numSplit: Int): (RDD[InstanceRow], Array[Float], Broadcast[FeatureInfo]) = {
    val transposeStart = System.currentTimeMillis()
    // 1. get #instance of each partition and collect labels
    val oriNumPart = oriData.getNumPartitions
    val partLabels = oriData.mapPartitionsWithIndex((partId, iterator) => {
      val partLabelsArr = iterator.map(_.label.toFloat).toArray
      Iterator((partId, partLabelsArr))
    }).collect()
    val (numInstances, partInsIdOffset) = {
      val tmp = partLabels.map(_._2.length).scanLeft(0)(_ + _)
      (tmp.last, tmp.slice(0, oriNumPart))
    }
    val labels = new Array[Float](numInstances)
    partLabels.foreach {
      case (partId, partLabelsArr) =>
        Array.copy(partLabelsArr, 0, labels, partInsIdOffset(partId), partLabelsArr.length)
    }
    val bcPartInsIdOffset = sc.broadcast(partInsIdOffset)
    // 2. generate candidate splits for each feature
    val toFPStart = System.currentTimeMillis()
    val evenPartitioner = new EvenPartitioner(numFeature, numWorker)
    val bcFeatureEdges = sc.broadcast(evenPartitioner.partitionEdges())
    // 2.1. create local quantile sketch
    val localSketchRdd = oriData.mapPartitions(iterator => {
      val sketches = new Array[HeapQuantileSketch](numFeature)
      for (fid <- 0 until numFeature)
        sketches(fid) = new HeapQuantileSketch()
      while (iterator.hasNext)
        iterator.next().feature
          .foreachActive((fid, fvalue) => sketches(fid).update(fvalue.toFloat))
      sketches.view.zipWithIndex.map {
        case (sketch, fid) => (fid, sketch)
      }.filter(!_._2.isEmpty)
        .iterator
    }, preservesPartitioning = true)
    // 2.2. repartition to executors evenly, merge as global quantile sketches
    // and collect quantiles as splits
    val splits = new Array[Array[Float]](numFeature)
    localSketchRdd.partitionBy(evenPartitioner)
      .mapPartitionsWithIndex((partId, iterator) => {
        val featLo = bcFeatureEdges.value(partId)
        val featHi = bcFeatureEdges.value(partId + 1)
        val sketches = new Array[HeapQuantileSketch](featHi - featLo)
        while (iterator.hasNext) {
          val (fid, sketch) = iterator.next()
          if (sketches(fid - featLo) == null)
            sketches(fid - featLo) = sketch
          else
            sketches(fid - featLo).merge(sketch)
        }
        val splits = sketches.map(sketch => {
          if (sketch == null) null
          else Maths.unique(sketch.getQuantiles(numSplit))
        })
        Iterator((partId, splits))
      }).collect()
      .foreach {
        case (partId, partSplits) =>
          val featLo = bcFeatureEdges.value(partId)
          for (i <- partSplits.indices)
            splits(featLo + i) = partSplits(i)
      }
    // 2.3. feature info
    val featureInfo = FeatureInfo(numFeature, splits)
    val bcFeatureInfo = sc.broadcast(featureInfo)
    // 3. repartition instances
    val data = oriData.mapPartitionsWithIndex((partId, iterator) => {
      val allSplits = bcFeatureInfo.value.splits
      val featureEdges = bcFeatureEdges.value
      val columnGroups = new Array[Array[(IntArrayList, ByteArrayList)]](numWorker)
      for (workerId <- 0 until numWorker) {
        val featLo = featureEdges(workerId)
        val featHi = featureEdges(workerId + 1)
        val group = (featLo until featHi).map(_ =>
          (new IntArrayList(), new ByteArrayList())).toArray
        columnGroups(workerId) = group
      }
      var curInsId = bcPartInsIdOffset.value(partId)
      val partitioner = new EvenPartitioner(numFeature, numWorker)
      while (iterator.hasNext) {
        iterator.next().feature.foreachActive((fid, fvalue) => {
          val workerId = partitioner.getPartition(fid)
          val featLo = featureEdges(workerId)
          val group = columnGroups(workerId)
          group(fid - featLo)._1.add(curInsId)
          val binId = Maths.indexOf(allSplits(fid), fvalue.toFloat)
          group(fid - featLo)._2.add((binId + Byte.MinValue).toByte)
        })
        curInsId += 1
      }
      columnGroups.view.zipWithIndex.map {
        case (group, workerId) =>
          (bcFeatureEdges.value(workerId), group.map(feature =>
            (feature._1.toIntArray(null), feature._2.toByteArray(null))))
      }.iterator
    }).partitionBy(evenPartitioner)
      .mapPartitionsWithIndex((partId, iterator) => {
        val instances = (0 until numInstances).map(_ =>
          (new IntArrayList(), new IntArrayList())).toArray
        iterator.toArray.sortBy(_._1).foreach {
          case (_, group) =>
            var curFid = bcFeatureEdges.value(partId)
            group.foreach {
              case (indices, bins) =>
                for (i <- indices.indices) {
                  val insId = indices(i)
                  instances(insId)._1.add(curFid)
                  val binId = bins(i).toInt - Byte.MinValue
                  instances(insId)._2.add(binId)
                }
                curFid += 1
            }
        }
        instances.map(ins => InstanceRow(ins._1.toIntArray(null), ins._2.toIntArray(null))).iterator
      })
    (data, labels, bcFeatureInfo)
  }

  def transpose(dpData: RDD[Instance], numFeature: Int, numWorker: Int,
                numSplit: Int): (RDD[Option[FeatureRow]], Array[Float], Broadcast[FeatureInfo]) = {
    // 1. get #instance of each partition, and calculate the offsets of instance indexes
    val oriNumPart = dpData.getNumPartitions
    val partNumIns = new Array[Int](oriNumPart)
    dpData.mapPartitionsWithIndex((partId, iterator) =>
      Iterator((partId, iterator.size)),
      preservesPartitioning = true
    ).collect()
      .foreach {
        case (partId, partSize) => partNumIns(partId) = partSize
      }
    val partInsIdOffset = new Array[Int](oriNumPart)
    for (i <- 1 until oriNumPart)
      partInsIdOffset(i) += partInsIdOffset(i - 1) + partNumIns(i - 1)
    val bcPartInsIdOffset = sc.broadcast(partInsIdOffset)
    // 2. collect labels of all instances
    val labels = new Array[Float](partNumIns.sum)
    dpData.mapPartitionsWithIndex((partId, iterator) =>
      Iterator((partId, iterator.map(_.label.toFloat).toArray)),
      preservesPartitioning = true
    ).collect()
      .foreach {
        case (partId, partLabels) => {
          require(partLabels.length == partNumIns(partId))
          val offset = partInsIdOffset(partId)
          Array.copy(partLabels, 0, labels, offset, partLabels.length)
        }
      }
    // 3. generate candidate splits for each feature
    val evenPartitioner = new EvenPartitioner(numFeature, numWorker)
    val bcFeatureEdges = sc.broadcast(evenPartitioner.partitionEdges())
    // 3.1. create local quantile sketches
    val localSketchRdd = dpData.mapPartitions(iterator => {
      val sketches = new Array[(Int, HeapQuantileSketch)](numFeature)
      for (fid <- 0 until numFeature)
        sketches(fid) = (fid, new HeapQuantileSketch())
      while (iterator.hasNext)
        iterator.next().feature.foreachActive((fid, fvalue) => sketches(fid)._2.update(fvalue.toFloat))
      sketches.filter(!_._2.isEmpty).iterator
    }, preservesPartitioning = true)
    // 3.2. repartition to executors evenly and merge as global quantile sketches
    val splitsRdd = localSketchRdd.repartitionAndSortWithinPartitions(evenPartitioner)
      .mapPartitions(iterator => {
        val splits = ArrayBuffer[(Int, Array[Float])]()
        var curFid = -1
        var curSketch = null.asInstanceOf[HeapQuantileSketch]
        while (iterator.hasNext) {
          val (fid, sketch) = iterator.next()
          if (fid != curFid) {
            if (curFid != -1)
              splits += ((curFid, Maths.unique(curSketch.getQuantiles(numSplit))))
            curSketch = sketch
            curFid = fid
          } else
            curSketch.merge(sketch)
        }
        if (curFid != -1)
          splits += ((curFid, Maths.unique(curSketch.getQuantiles(numSplit))))
        splits.iterator
      }, preservesPartitioning = true)
    // 3.3. collect candidate splits for all features
    val splits = new Array[Array[Float]](numFeature)
    splitsRdd.collect().foreach { case (fid, fsplits) => splits(fid) = fsplits }
    // 3.4. feature info
    val featureInfo = FeatureInfo(numFeature, splits)
    val bcFeatureInfo = sc.broadcast(featureInfo)
    // 4. transpose instances
    // 4.1. map feature values into bin indexes and transpose local data
    val mediumFeatRowRdd = dpData.mapPartitionsWithIndex((partId, iterator) => {
      val splits = bcFeatureInfo.value.splits
      val insIdLists = new Array[IntArrayList](numFeature)
      val binIdLists = new Array[ByteArrayList](numFeature)
      for (fid <- 0 until numFeature) {
        insIdLists(fid) = new IntArrayList()
        binIdLists(fid) = new ByteArrayList()
      }
      var curInsId = bcPartInsIdOffset.value(partId)
      while (iterator.hasNext) {
        iterator.next().feature.foreachActive((fid, fvalue) => {
          insIdLists(fid).add(curInsId)
          val binId = Maths.indexOf(splits(fid), fvalue.toFloat)
          binIdLists(fid).add((binId + Byte.MinValue).toByte)
        })
        curInsId += 1
      }
      val mediumFeatRows = new ArrayBuffer[(Int, Option[(Array[Int], Array[Byte])])](numFeature)
      for (fid <- 0 until numFeature) {
        val mediumFeatRow = if (insIdLists(fid).size() > 0) {
          val featIndices = insIdLists(fid).toIntArray(null)
          val featBins = binIdLists(fid).toByteArray(null)
          (fid, Option((featIndices, featBins)))
        } else {
          (fid, Option.empty)
        }
        mediumFeatRows += mediumFeatRow
      }
      mediumFeatRows.iterator
    })
    // 4.2. repartition feature rows evenly, compact medium feature rows
    // of one feature (from different partition) into one
    val fpData = mediumFeatRowRdd.repartitionAndSortWithinPartitions(evenPartitioner)
      .mapPartitionsWithIndex((partId, iterator) => {
        val featLo = bcFeatureEdges.value(partId)
        val featHi = bcFeatureEdges.value(partId + 1)
        val featureRows = new ArrayBuffer[Option[FeatureRow]](featHi - featLo)
        val partFeatRows = collection.mutable.ArrayBuffer[FeatureRow]()
        var curFid = featLo
        while (iterator.hasNext) {
          val (fid, partFeatRow) = iterator.next()
          val mediumFeatRow = partFeatRow match {
            case Some((indices, bins)) => FeatureRow(indices, bins.map(_.toInt - Byte.MinValue))
            case None => FeatureRow(null, null)
          }
          if (fid != curFid) {
            featureRows += FeatureRow.compact(partFeatRows)
            partFeatRows.clear()
            curFid = fid
            partFeatRows += mediumFeatRow
          } else if (!iterator.hasNext) {
            partFeatRows += mediumFeatRow
            featureRows += FeatureRow.compact(partFeatRows)
          } else {
            partFeatRows += mediumFeatRow
          }
        }
        require(featureRows.size == featHi - featLo)
        featureRows.iterator
      })
    (fpData, labels, bcFeatureInfo)
  }
}
