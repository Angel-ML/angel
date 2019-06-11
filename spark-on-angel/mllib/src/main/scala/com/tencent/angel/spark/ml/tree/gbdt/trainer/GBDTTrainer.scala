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


package com.tencent.angel.spark.ml.tree.gbdt.trainer

import com.tencent.angel.conf.AngelConf
import com.tencent.angel.ml.core.conf.{MLConf, SharedConf}
import com.tencent.angel.spark.ml.core.ArgsUtil
import com.tencent.angel.spark.ml.tree.param.GBDTParam
import com.tencent.angel.spark.ml.tree.gbdt.dataset.Dataset
import com.tencent.angel.spark.ml.tree.gbdt.dataset.Dataset._
import com.tencent.angel.spark.ml.tree.data.Instance
import com.tencent.angel.spark.ml.tree.gbdt.metadata.FeatureInfo
import com.tencent.angel.spark.ml.tree.gbdt.tree.{GBTSplit, GBTTree}
import com.tencent.angel.spark.ml.tree.objective.ObjectiveFactory
import com.tencent.angel.spark.ml.tree.objective.metric.EvalMetric.Kind
import com.tencent.angel.spark.ml.tree.sketch.HeapQuantileSketch
import com.tencent.angel.spark.ml.tree.util.{DataLoader, LogHelper, Maths}
import com.tencent.angel.spark.ml.util.SparkUtils
import org.apache.hadoop.fs.Path
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.{Partitioner, SparkConf, SparkContext, TaskContext}

import scala.collection.mutable.{ArrayBuilder => AB}


object GBDTTrainer {

  def main(args: Array[String]): Unit = {

    @transient val conf = new SparkConf()

    val param = new GBDTParam

    // spark conf
    val numExecutor = SparkUtils.getNumExecutors(conf)
    val numCores = conf.get("spark.executor.cores").toInt
    param.numWorker = numExecutor
    param.numThread = numCores
    conf.set("spark.task.cpus", numCores.toString)
    conf.set("spark.locality.wait", "0")
    conf.set("spark.memory.fraction", "0.7")
    conf.set("spark.memory.storageFraction", "0.8")
    conf.set("spark.task.maxFailures", "1")
    conf.set("spark.yarn.maxAppAttempts", "1")
    conf.set("spark.network.timeout", "1000")
    conf.set("spark.executor.heartbeatInterval", "500")

    val params = ArgsUtil.parse(args)

    // dataset conf
    param.taskType = params.getOrElse(MLConf.ML_GBDT_TASK_TYPE, MLConf.DEFAULT_ML_GBDT_TASK_TYPE)
    param.numClass = params.getOrElse(MLConf.ML_NUM_CLASS, "2").toInt
    param.numFeature = params.getOrElse(MLConf.ML_FEATURE_INDEX_RANGE, "-1").toInt
    SharedConf.get().setInt(MLConf.ML_NUM_CLASS, param.numClass)
    SharedConf.get().setInt(MLConf.ML_FEATURE_INDEX_RANGE, param.numFeature)

    // loss and metric
    param.lossFunc = params.getOrElse(MLConf.ML_GBDT_LOSS_FUNCTION, "binary:logistic")
    param.evalMetrics = params.getOrElse(MLConf.ML_GBDT_EVAL_METRIC, "error").split(",").map(_.trim).filter(_.nonEmpty)
    SharedConf.get().set(MLConf.ML_GBDT_LOSS_FUNCTION, param.lossFunc)

    param.taskType match {
      case "regression" =>
        require(param.lossFunc.equals("rmse") && param.evalMetrics(0).equals("rmse"),
          "loss function and metric of regression task should be rmse")
        param.numClass = 2
      case "classification" =>
        require(param.numClass >= 2, "number of labels should be larger than 2")
        param.multiStrategy = params.getOrElse("ml.gbdt.multi.class.strategy", "one-tree")
        if (param.isMultiClassMultiTree) param.lossFunc = "binary:logistic"
        param.multiGradCache = params.getOrElse("ml.gbdt.multi.class.grad.cache", "true").toBoolean
    }

    // major algo conf
    param.featSampleRatio = params.getOrElse(MLConf.ML_GBDT_FEATURE_SAMPLE_RATIO, "1.0").toFloat
    SharedConf.get().setFloat(MLConf.ML_GBDT_FEATURE_SAMPLE_RATIO, param.featSampleRatio)
    param.learningRate = params.getOrElse(MLConf.ML_LEARN_RATE, "0.1").toFloat
    SharedConf.get().setFloat(MLConf.ML_LEARN_RATE, param.learningRate)
    param.numSplit = params.getOrElse(MLConf.ML_GBDT_SPLIT_NUM, "10").toInt
    SharedConf.get().setInt(MLConf.ML_GBDT_SPLIT_NUM, param.numSplit)
    param.numTree = params.getOrElse(MLConf.ML_GBDT_TREE_NUM, "20").toInt
    if (param.isMultiClassMultiTree) param.numTree *= param.numClass
    SharedConf.get().setInt(MLConf.ML_GBDT_TREE_NUM, param.numTree)
    param.maxDepth = params.getOrElse(MLConf.ML_GBDT_TREE_DEPTH, "7").toInt
    SharedConf.get().setInt(MLConf.ML_GBDT_TREE_DEPTH, param.maxDepth)
    val maxNodeNum = Maths.pow(2, param.maxDepth + 1) - 1
    param.maxNodeNum = params.getOrElse(MLConf.ML_GBDT_MAX_NODE_NUM, "4096").toInt min maxNodeNum
    SharedConf.get().setInt(MLConf.ML_GBDT_MAX_NODE_NUM, param.maxNodeNum)

    // less important algo conf
    //param.histSubtraction = angelConf.getBoolean(MLConf.ML_GBDT_HIST_SUBTRACTION, MLConf.DEFAULT_ML_GBDT_HIST_SUBTRACTION)
    param.histSubtraction = params.getOrElse(MLConf.ML_GBDT_HIST_SUBTRACTION, "true").toBoolean
    param.lighterChildFirst = params.getOrElse(MLConf.ML_GBDT_LIGHTER_CHILD_FIRST, "true").toBoolean
    param.fullHessian = params.getOrElse(MLConf.ML_GBDT_FULL_HESSIAN, "false").toBoolean
    param.minChildWeight = params.getOrElse(MLConf.ML_GBDT_MIN_CHILD_WEIGHT, "0.01").toFloat
    param.minNodeInstance = params.getOrElse(MLConf.ML_GBDT_MIN_NODE_INSTANCE, "1024").toInt
    param.minSplitGain = params.getOrElse(MLConf.ML_GBDT_MIN_SPLIT_GAIN, "0.0").toFloat
    param.regAlpha = params.getOrElse(MLConf.ML_GBDT_REG_ALPHA, "0.0").toFloat
    param.regLambda = params.getOrElse(MLConf.ML_GBDT_REG_LAMBDA, "1.0").toFloat
    param.maxLeafWeight = params.getOrElse(MLConf.ML_GBDT_MAX_LEAF_WEIGHT, "0.0").toFloat

    println(s"Hyper-parameters:\n$param")

    val trainPath = params.getOrElse(AngelConf.ANGEL_TRAIN_DATA_PATH, "xxx")
    val validPath = params.getOrElse(AngelConf.ANGEL_VALIDATE_DATA_PATH, "xxx")
    val modelPath = params.getOrElse(AngelConf.ANGEL_SAVE_MODEL_PATH, "xxx")

    @transient implicit val sc = new SparkContext(conf)

    try {
      val trainer = new GBDTTrainer(param)
      trainer.initialize(trainPath, validPath)
      val model = trainer.train()
      trainer.save(model, modelPath)
    } catch {
      case e: Exception =>
        e.printStackTrace()
    } finally {
    }
  }

  def roundRobinFeatureGrouping(numFeature: Int, numGroup: Int): (Array[Int], Array[Array[Int]]) = {
    val fidToGroupId = new Array[Int](numFeature)
    val buffers = new Array[AB.ofInt](numGroup)
    for (partId <- 0 until numGroup) {
      buffers(partId) = new AB.ofInt
      buffers(partId).sizeHint((1.5 * numFeature / numGroup).toInt)
    }
    for (fid <- 0 until numFeature) {
      val partId = fid % numGroup
      fidToGroupId(fid) = partId
      buffers(partId) += fid
    }
    val groupIdToFid = buffers.map(_.result())
    (fidToGroupId, groupIdToFid)
  }

  def balancedFeatureGrouping(featNNZ: Array[Int], numGroup: Int): (Array[Int], Array[Array[Int]], Array[Int], Array[Int]) = {
    val numFeature = featNNZ.length
    val fidToGroupId = new Array[Int](numFeature)
    val groupSizes = new Array[Int](numGroup)
    val groupNNZ = new Array[Long](numGroup)
    val sortedFeatNNZ = featNNZ.zipWithIndex.sortBy(_._1)
    for (i <- 0 until (numFeature / 2)) {
      val fid = sortedFeatNNZ(i)._2
      val groupId = fid % numGroup
      fidToGroupId(fid) = groupId
      groupSizes(groupId) += 1
      groupNNZ(groupId) += sortedFeatNNZ(i)._1
    }
    for (i <- (numFeature / 2) until numFeature) {
      val fid = sortedFeatNNZ(i)._2
      val groupId = numGroup - (fid % numGroup) - 1
      fidToGroupId(fid) = groupId
      groupSizes(groupId) += 1
      groupNNZ(groupId) += sortedFeatNNZ(i)._1
    }
    val fidToNewFid = new Array[Int](numFeature)
    val groupIdToFid = groupSizes.map(groupSize => new Array[Int](groupSize))
    val curIndexes = new Array[Int](numGroup)
    for (fid <- fidToGroupId.indices) {
      val groupId = fidToGroupId(fid)
      val newFid = curIndexes(groupId)
      fidToNewFid(fid) = newFid
      groupIdToFid(groupId)(newFid) = fid
      curIndexes(groupId) += 1
    }
    println("Feature grouping info: " + (groupSizes, groupNNZ, 0 until numGroup).zipped.map {
      case (size, nnz, groupId) => s"(group[$groupId] #feature[$size] #nnz[$nnz])"
    }.mkString(" "))
    (fidToGroupId, groupIdToFid, groupSizes, fidToNewFid)
  }

  def featureInfoOfGroup(featureInfo: FeatureInfo, groupId: Int,
                         groupIdToFid: Array[Int]): FeatureInfo = {
    val groupSize = groupIdToFid.length
    val featTypes = new Array[Boolean](groupSize)
    val numBin = new Array[Int](groupSize)
    val splits = new Array[Array[Float]](groupSize)
    val defaultBins = new Array[Int](groupSize)
    groupIdToFid.view.zipWithIndex.foreach {
      case (fid, newFid) =>
        featTypes(newFid) = featureInfo.isCategorical(fid)
        numBin(newFid) = featureInfo.getNumBin(fid)
        splits(newFid) = featureInfo.getSplits(fid)
        defaultBins(newFid) = featureInfo.getDefaultBin(fid)
    }
    FeatureInfo(featTypes, numBin, splits, defaultBins)
  }
}

import GBDTTrainer._

class GBDTTrainer(param: GBDTParam) extends Serializable {
  @transient implicit val sc = SparkContext.getOrCreate()

  @transient private[gbdt] var bcFidToGroupId: Broadcast[Array[Int]] = _
  @transient private[gbdt] var bcGroupIdToFid: Broadcast[Array[Array[Int]]] = _
  @transient private[gbdt] var bcFidToNewFid: Broadcast[Array[Int]] = _
  @transient private[gbdt] var bcGroupSizes: Broadcast[Array[Int]] = _
  @transient private[gbdt] var bcFeatureInfo: Broadcast[FeatureInfo] = _

  @transient private[gbdt] var workers: RDD[FPGBDTTrainerWrapper] = _

  private[gbdt] var numTrain: Int = _
  private[gbdt] var numValid: Int = _

  def initialize(trainInput: String, validInput: String)
                (implicit sc: SparkContext): Unit = {
    val initStart = System.currentTimeMillis()
    val bcParam = sc.broadcast(param)
    val numFeature = param.numFeature
    val numWorker = param.numWorker
    val numSplit = param.numSplit

    // 1. load data from hdfs
    val loadStart = System.currentTimeMillis()
    val trainDP = fromTextFile(trainInput, numFeature, numWorker)
      .coalesce(numWorker)
      .mapPartitions(iterator => Iterator(Dataset[Int, Float](iterator.toSeq)))
      .persist()
    val numTrain = trainDP.map(_.size).collect().sum
    println(s"Load data cost ${System.currentTimeMillis() - loadStart} ms")

    // 2. collect labels, ensure 0-based indexed and broadcast
    val labelStart = System.currentTimeMillis()
    val labels = new Array[Float](numTrain)
    val partLabels = trainDP.map(dataset =>
      (TaskContext.getPartitionId(), dataset.getLabels)
    ).collect()
    require(partLabels.map(_._1).distinct.length == partLabels.length
      && partLabels.map(_._2).forall(_.isDefined))
    var offset = 0
    partLabels.sortBy(_._1).map(_._2.get).foreach(partLabel => {
      Array.copy(partLabel, 0, labels, offset, partLabel.length)
      offset += partLabel.length
    })
    val changeLabel = if (param.isClassification) Instance.ensureLabel(labels, param.numClass) else false
    val bcLabels = sc.broadcast(labels)
    val bcChangeLabel = sc.broadcast(changeLabel)
    println(s"Collect labels cost ${System.currentTimeMillis() - labelStart} ms")

    // IdenticalPartitioner for shuffle operation
    class IdenticalPartitioner extends Partitioner {
      override def numPartitions: Int = numWorker

      override def getPartition(key: Any): Int = {
        val partId = key.asInstanceOf[Int]
        require(partId < numWorker, s"Partition id $partId exceeds maximum partition $numWorker")
        partId
      }
    }

    // 3. build quantile sketches, get candidate splits,
    // and create feature info, finally broadcast info to all workers
    val getSplitsStart = System.currentTimeMillis()
    val isCategorical = new Array[Boolean](numFeature)
    val splits = new Array[Array[Float]](numFeature)
    val featNNZ = new Array[Int](numFeature)
    trainDP.flatMap(dataset => {
      val sketchGroups = new Array[Array[HeapQuantileSketch]](numWorker)
      (0 until numWorker).foreach(groupId => {
        val groupSize = numFeature / numWorker + (if (groupId < (numFeature % numWorker)) 1 else 0)
        sketchGroups(groupId) = new Array[HeapQuantileSketch](groupSize)
      })
      val sketches = createSketches(dataset, numFeature)
      val curIndex = new Array[Int](numWorker)
      for (fid <- 0 until numFeature) {
        val groupId = fid % numWorker
        if (sketches(fid) == null || sketches(fid).isEmpty) {
          sketchGroups(groupId)(curIndex(groupId)) = null
        } else {
          sketchGroups(groupId)(curIndex(groupId)) = sketches(fid)
        }
        curIndex(groupId) += 1
      }
      sketchGroups.zipWithIndex.map {
        case (group, groupId) => (groupId, group)
      }.iterator
    }).partitionBy(new IdenticalPartitioner)
      .mapPartitions(iterator => {
        // merge quantile sketches and get quantiles as candidate splits
        val (groupIds, sketchGroups) = iterator.toArray.unzip
        val groupId = groupIds.head
        require(groupIds.forall(_ == groupId))
        val merged = sketchGroups.head
        val tail = sketchGroups.tail
        val size = merged.length
        val splits = (0 until size).map(i => {
          tail.foreach(group => {
            if (merged(i) == null || merged(i).isEmpty) {
              merged(i) = group(i)
            } else {
              merged(i).merge(group(i))
            }
          })
          if (merged(i) != null && !merged(i).isEmpty) {
            val distinct = merged(i).tryDistinct(FeatureInfo.ENUM_THRESHOLD)
            if (distinct == null) {
              val tmpSplits = Maths.unique(merged(i).getQuantiles(numSplit))
              if (tmpSplits.length == 1 && tmpSplits(0) > 0) {
                (false, Array(0, tmpSplits(0)), merged(i).getN.toInt)
              } else if (tmpSplits.length == 1 && tmpSplits(0) < 0) {
                (false, Array(tmpSplits(0), 0), merged(i).getN.toInt)
              } else {
                (false, tmpSplits, merged(i).getN.toInt)
              }
            }
            else {
              (true, distinct, merged(i).getN.toInt)
            }
          } else {
            (false, null, 0)
          }
        })
        Iterator((groupId, splits))
      }, preservesPartitioning = true)
      .collect()
      .foreach {
        case (groupId, groupSplits) =>
          // restore feature id based on column grouping info
          // and set splits to corresponding feature
          groupSplits.view.zipWithIndex.foreach {
            case ((fIsCategorical, fSplits, nnz), index) =>
              val fid = index * numWorker + groupId
              isCategorical(fid) = fIsCategorical
              splits(fid) = fSplits
              featNNZ(fid) = nnz
          }
      }
    val featureInfo = FeatureInfo(isCategorical, splits)
    val bcFeatureInfo = sc.broadcast(featureInfo)
    println(s"Create feature info cost ${System.currentTimeMillis() - getSplitsStart} ms")

    // 4. Partition features into groups,
    // get feature id to group id mapping and inverted indexing
    val featGroupStart = System.currentTimeMillis()
    val (fidToGroupId, groupIdToFid, groupSizes, fidToNewFid) = balancedFeatureGrouping(featNNZ, numWorker)
    val bcFidToGroupId = sc.broadcast(fidToGroupId)
    val bcGroupIdToFid = sc.broadcast(groupIdToFid)
    val bcFidToNewFid = sc.broadcast(fidToNewFid)
    val bcGroupSizes = sc.broadcast(groupSizes)
    println(s"Balanced feature grouping cost ${System.currentTimeMillis() - featGroupStart} ms")

    // 5. Perform horizontal-to-vertical partitioning
    val repartStart = System.currentTimeMillis()
    val trainFP = trainDP.flatMap(dataset => {
      // turn (feature index, feature value) into (feature index, bin index)
      // and partition into column groups
      columnGrouping(dataset, bcFidToGroupId.value,
        bcFidToNewFid.value, bcFeatureInfo.value, numWorker)
        .zipWithIndex.map {
        case (group, groupId) => (groupId, (TaskContext.getPartitionId(), group))
      }
    }).partitionBy(new IdenticalPartitioner)
      .mapPartitions(iterator => {
        // merge same group into a dataset
        val (groupIds, columnGroups) = iterator.toArray.unzip
        val groupId = groupIds.head
        require(groupIds.forall(_ == groupId))
        val partIds = columnGroups.map(_._1)
        require(partIds.distinct.length == partIds.length)
        Iterator(Dataset[Short, Byte](columnGroups.sortBy(_._1).map(_._2)))
      }).cache()
    require(trainFP.map(_.size).collect().forall(_ == numTrain))
    println(s"Repartitioning cost ${System.currentTimeMillis() - repartStart} ms")
    trainDP.unpersist() // persist FP and unpersist DP to save memory

    // 6. initialize worker
    val initWorkerStart = System.currentTimeMillis()
    val valid = DataLoader.loadLibsvmDP(validInput, param.numFeature)
      .repartition(param.numWorker)
    val workers = trainFP.zipPartitions(valid, preservesPartitioning = true)(
      (trainIter, validIter) => {
        val train = trainIter.toArray
        require(train.length == 1)
        val trainData = Dataset.restore(train.head)
        val trainLabels = bcLabels.value
        val valid = validIter.toArray
        val validData = valid.map(_.feature)
        val validLabels = valid.map(_.label.toFloat)
        if (bcChangeLabel.value) Instance.changeLabel(validLabels, bcParam.value.numClass)
        val workerId = TaskContext.getPartitionId
        val worker = new FPGBDTTrainer(workerId, bcParam.value,
          featureInfoOfGroup(bcFeatureInfo.value, workerId, bcGroupIdToFid.value(workerId)),
          trainData, trainLabels, validData, validLabels)
        val wrapper = FPGBDTTrainerWrapper(workerId, worker)
        Iterator(wrapper)
      }
    ).cache()
    workers.foreach(worker =>
      println(s"Worker[${worker.workerId}] initialization done"))
    val numValid = workers.map(_.validLabels.length).collect().sum
    trainFP.unpersist()
    println(s"Initialize workers done, cost ${System.currentTimeMillis() - initWorkerStart} ms, " +
      s"$numTrain train data, $numValid valid data")

    this.bcFidToGroupId = bcFidToGroupId
    this.bcGroupIdToFid = bcGroupIdToFid
    this.bcFidToNewFid = bcFidToNewFid
    this.bcGroupSizes = bcGroupSizes
    this.bcFeatureInfo = bcFeatureInfo
    this.workers = workers
    this.numTrain = numTrain
    this.numValid = numValid

    println(s"Initialization done, cost ${System.currentTimeMillis() - initStart} ms in total")
  }

  def train(): Seq[GBTTree] = {
    val trainStart = System.currentTimeMillis()

    val loss = ObjectiveFactory.getLoss(param.lossFunc)
    val evalMetrics = ObjectiveFactory.getEvalMetricsOrDefault(param.evalMetrics, loss)
    //val multiStrategy = ObjectiveFactory.getMultiStrategy(param.multiStrategy)

    LogHelper.setLogLevel("info")

    for (treeId <- 0 until param.numTree) {
      LogHelper.print(s"Start to train tree ${treeId + 1}")

      // 1. create new tree
      val createStart = System.currentTimeMillis()
      workers.foreach(_.createNewTree())
      val bestSplits = new Array[GBTSplit](Maths.pow(2, param.maxDepth) - 1)
      val bestOwnerIds = new Array[Int](Maths.pow(2, param.maxDepth) - 1)
      val bestAliasFids = new Array[Int](Maths.pow(2, param.maxDepth) - 1)
      LogHelper.print(s"Tree[${treeId + 1}] Create new tree cost ${System.currentTimeMillis() - createStart} ms")

      // 2. iteratively build one tree
      var hasActive = true
      while (hasActive) {
        // 2.1. build histograms and find local best splits
        val findStart = System.currentTimeMillis()
        val nids = collection.mutable.TreeSet[Int]()
        workers.map(worker => (worker.workerId, worker.findSplits()))
          .collect().foreach {
          case (workerId, splits) =>
            splits.foreach {
              case (nid, split) =>
                nids += nid
                if (bestSplits(nid) == null || bestSplits(nid).needReplace(split)) {
                  val fidInWorker = split.getSplitEntry.getFid
                  val trueFid = bcGroupIdToFid.value(workerId)(fidInWorker)
                  split.getSplitEntry.setFid(trueFid)
                  bestSplits(nid) = split
                  bestOwnerIds(nid) = workerId
                  bestAliasFids(nid) = fidInWorker
                }
            }
        }
        // (nid, ownerId, fidInWorker, split)
        val gatheredSplits = nids.toArray.map(nid => (nid,
          bestOwnerIds(nid), bestAliasFids(nid), bestSplits(nid)))
        val validSplits = gatheredSplits.filter(_._4.isValid(param.minSplitGain))
        val leaves = gatheredSplits.filter(!_._4.isValid(param.minSplitGain)).map(_._1)
        if (gatheredSplits.nonEmpty) {
          LogHelper.print(s"Build histograms and find best splits cost " +
            s"${System.currentTimeMillis() - findStart} ms, " +
            s"${validSplits.length} node(s) to split")
          val resultStart = System.currentTimeMillis()
          val bcValidSplits = sc.broadcast(validSplits)
          val bcLeaves = sc.broadcast(leaves)
          val splitResults = workers.flatMap(worker => {
            bcLeaves.value.foreach(worker.setAsLeaf)
            worker.getSplitResults(bcValidSplits.value).iterator
          }).collect()
          val bcSplitResults = sc.broadcast(splitResults)
          LogHelper.print(s"Get split results cost ${System.currentTimeMillis() - resultStart} ms")
          // 2.3. split nodes
          val splitStart = System.currentTimeMillis()
          hasActive = workers.map(_.splitNodes(bcSplitResults.value)).collect()(0)
          bcSplitResults.destroy()
          LogHelper.print(s"Split nodes cost ${System.currentTimeMillis() - splitStart} ms")
        } else {
          // no active nodes
          hasActive = false
        }
      }

      // 3. finish tree
      val finishStart = System.currentTimeMillis()
      val trainMetrics = new Array[Double](evalMetrics.length)
      val validMetrics = new Array[Double](evalMetrics.length)
      workers.map(worker => {
        worker.finishTree()
        worker.evaluate()
      }).collect().foreach(_.zipWithIndex.foreach {
        case ((kind, train, valid), index) =>
          require(kind == evalMetrics(index).getKind)
          trainMetrics(index) += train
          validMetrics(index) += valid
      })
      if (! (param.isMultiClassMultiTree && (treeId + 1) % param.numClass != 0) ) {
        val evalTrainMsg = (evalMetrics, trainMetrics).zipped.map {
          case (evalMetric, trainSum) => evalMetric.getKind match {
            case Kind.AUC => s"${evalMetric.getKind}[${evalMetric.avg(trainSum, workers.count.toInt)}]"
            case _ => s"${evalMetric.getKind}[${evalMetric.avg(trainSum, numTrain)}]"
          }
        }.mkString(", ")
        val round = if (param.isMultiClassMultiTree) (treeId + 1) / param.numClass else (treeId + 1)
        println(s"Evaluation on train data after ${round} tree(s): $evalTrainMsg")
        val evalValidMsg = (evalMetrics, validMetrics).zipped.map {
          case (evalMetric, validSum) => evalMetric.getKind match {
            case Kind.AUC => s"${evalMetric.getKind}[${evalMetric.avg(validSum, workers.count.toInt)}]"
            case _ => s"${evalMetric.getKind}[${evalMetric.avg(validSum, numValid)}]"
          }
        }.mkString(", ")
        println(s"Evaluation on valid data after ${round} tree(s): $evalValidMsg")
        LogHelper.print(s"Tree[${round}] Finish tree cost ${System.currentTimeMillis() - finishStart} ms")
        val currentTime = System.currentTimeMillis()
        println(s"Train tree cost ${currentTime - createStart} ms, " +
          s"${round} tree(s) done, ${currentTime - trainStart} ms elapsed")
      }
      //      workers.map(_.reportTime()).collect().zipWithIndex.foreach {
      //        case (str, id) =>
      //          println(s"========Time cost summation of worker[$id]========")
      //          println(str)
      //      }
    }

    // TODO: check equality
    val forest = workers.map(_.finalizeModel()).collect()(0)
    forest.zipWithIndex.foreach {
      case (tree, treeId) =>
        println(s"Tree[${treeId + 1}] contains ${tree.size} nodes " +
          s"(${(tree.size - 1) / 2 + 1} leaves)")
    }
    forest
  }

  def save(model: Seq[GBTTree], modelPath: String)(implicit sc: SparkContext): Unit = {
    val path = new Path(modelPath)
    val fs = path.getFileSystem(sc.hadoopConfiguration)
    if (fs.exists(path)) fs.delete(path, true)
    println(s"Model will be saved to $modelPath")
    sc.parallelize(Seq(model)).saveAsObjectFile(modelPath)
  }

}
