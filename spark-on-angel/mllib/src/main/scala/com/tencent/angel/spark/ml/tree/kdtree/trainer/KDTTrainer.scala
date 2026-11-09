package com.tencent.angel.spark.ml.tree.kdtree.trainer


import com.tencent.angel.spark.ml.tree.kdtree.dataset.Dataset._
import com.tencent.angel.spark.ml.tree.gbdt.metadata.FeatureInfo
import com.tencent.angel.spark.ml.tree.kdtree.tree.{KDTSplit, KDTree}
import com.tencent.angel.spark.ml.tree.param.KDTParam
import com.tencent.angel.spark.ml.tree.sketch.HeapQuantileSketch
import com.tencent.angel.spark.ml.tree.util.{LogHelper, Maths}
import org.apache.spark.{Partitioner, SparkContext, TaskContext}
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import com.tencent.angel.ml.math2.vector.IntFloatVector
import com.tencent.angel.spark.ml.tree.kdtree.dataset.Dataset
import com.tencent.angel.spark.ml.tree.kdtree.metadata.InstanceInfo
import org.apache.hadoop.fs.Path
import KDTTrainer._
import com.tencent.angel.spark.context.PSContext
import com.tencent.angel.spark.ml.tree.kdtree.psmodel.KDTreePSModel
import org.apache.spark.storage.StorageLevel

import scala.collection.mutable.ArrayBuffer


class KDTTrainer(param: KDTParam) extends Serializable {
  @transient implicit val sc = SparkContext.getOrCreate()

  @transient private var bcFidToGroupId: Broadcast[Array[Int]] = _
  @transient private var bcGroupIdToFid: Broadcast[Array[Array[Int]]] = _
  @transient private var bcFidToNewFid: Broadcast[Array[Int]] = _
  @transient private var bcGroupSizes: Broadcast[Array[Int]] = _
  @transient private var bcFeatureInfo: Broadcast[FeatureInfo] = _
//  @transient private var bcLabels: Broadcast[Array[Int]] = _
  private var labelsRDD: RDD[Int] = _

  @transient private var workers: RDD[FPKDTTrainerWrapper] = _
  private var numTrain: Int = _
  private val ENUM_THRESHOLD = 1
  var minLabel: Int = _
  var maxLabel: Int = _


  def initialize(trainInput: String, dataType: String)
                (implicit sc: SparkContext): Unit = {
    val initStart = System.currentTimeMillis()
    val bcParam = sc.broadcast(param)
    val numFeature = param.numFeature
    val numWorker = param.numWorker
    val numSplit = param.numSplit

    // 1. load data from hdfs
    val loadStart = System.currentTimeMillis()
    val trainDP = fromTextFile(trainInput, numFeature, numWorker, dataType)
      .coalesce(numWorker)
      .mapPartitions(iterator => Iterator(Dataset[Int, Float](iterator.toSeq)))
      .persist()
    val numTrain = trainDP.map(_.size).collect().sum
    println(numTrain)
    println(s"Load data cost ${System.currentTimeMillis() - loadStart} ms")

    labelsRDD = trainDP.map(dataset =>
      (TaskContext.getPartitionId(), dataset.getLabels)
    ).sortBy(_._1).map(_._2.get).flatMap(partLabel => partLabel).map(_.toInt)//.collect()
    labelsRDD.persist(StorageLevel.DISK_ONLY)

    minLabel = labelsRDD.min
    maxLabel = labelsRDD.max
//    val bcLabels = sc.broadcast(labels)


    // IdenticalPartitioner for shuffle operationFPKDTTrainerWrapper
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

    val splits = new Array[Array[Float]](numFeature)
    val counts = new Array[Array[Int]](numFeature)
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
            val distinct = merged(i).tryDistinct(ENUM_THRESHOLD)
            if (distinct == null) {
              val tmpSplits = Maths.unique(merged(i).getMedianQuantilies(numSplit, numTrain))
              val tmpCounts = Maths.unique(merged(i).getCount().map(_.toFloat)).map(_.toInt)
              print("splits: ")
              tmpSplits.foreach(x => print(s"${x}, "))
              print("\ncounts: ")
              tmpCounts.foreach(x => print(s"${x}, "))
              println()
              if (tmpSplits.length == 1 && tmpSplits(0) > 0) {
                (false, Array(0, tmpSplits(0)), Array(0, tmpCounts(0)), merged(i).getN.toInt)
              } else if (tmpSplits.length == 1 && tmpSplits(0) < 0) {
                (false, Array(tmpSplits(0), 0), Array(tmpCounts(0), 0), merged(i).getN.toInt)
              } else {
                (false, tmpSplits, tmpCounts, merged(i).getN.toInt)
              }
            }
            else {
              (true, distinct, null, merged(i).getN.toInt)
            }
          } else {
            (false, null, null, 0)
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
            case ((_, fSplits, fCounts, nnz), index) =>
              val fid = index * numWorker + groupId
              splits(fid) = fSplits
              counts(fid) = fCounts
              featNNZ(fid) = nnz
          }
      }
    val featureInfo = FeatureInfo(numFeature, splits, ENUM_THRESHOLD, counts)
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
      columnGroupingFeature(dataset, bcFidToGroupId.value,
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
    val workers = trainFP.mapPartitions(
      trainIter => {
        val train = trainIter.toArray
        require(train.length == 1)
        val trainData = Dataset.restore(train.head)
        val workerId = TaskContext.getPartitionId
        val worker = new FPKDTTrainer(workerId, bcParam.value,
          featureInfoOfGroup(bcFeatureInfo.value, workerId, bcGroupIdToFid.value(workerId)),
          trainData)
        val wrapper = FPKDTTrainerWrapper(workerId, worker)
        Iterator(wrapper)
      }
    ).cache()
    workers.foreach(worker =>
      println(s"Worker[${worker.workerId}] initialization done"))
    trainFP.unpersist()
    println(s"Initialize workers done, cost ${System.currentTimeMillis() - initWorkerStart} ms, " +
      s"$numTrain train data")

    this.bcFidToGroupId = bcFidToGroupId
    this.bcGroupIdToFid = bcGroupIdToFid
    this.bcFidToNewFid = bcFidToNewFid
    this.bcGroupSizes = bcGroupSizes
    this.bcFeatureInfo = bcFeatureInfo
    this.workers = workers
    this.numTrain = numTrain
//    this.bcLabels = bcLabels

    println(s"Initialization done, cost ${System.currentTimeMillis() - initStart} ms in total")
  }

  def train(batchSize: Int, numPsPartitions: Int): (Seq[KDTree], InstanceInfo, RDD[(Array[Long], Array[IntFloatVector])], Array[Int]) = {

    LogHelper.setLogLevel("info")
    val treeId = 0

    // 1. create new tree
    val createStart = System.currentTimeMillis()
    workers.foreach(_.createNewTree())
    val bestSplits = new Array[KDTSplit](Maths.pow(2, param.maxDepth) - 1)
    val bestOwnerIds = new Array[Int](Maths.pow(2, param.maxDepth) - 1)
    val bestAliasFids = new Array[Int](Maths.pow(2, param.maxDepth) - 1)
    LogHelper.print(s"Tree[${treeId + 1}] Create new tree cost ${System.currentTimeMillis() - createStart} ms")

    // 2. iteratively build one tree
    var hasActive = true
    val activeNodes = ArrayBuffer[Int]()
    while (hasActive || activeNodes.nonEmpty) {
      // 2.1. build histograms and find local best splits
      val nids = collection.mutable.TreeSet[Int]()
      if (hasActive) {
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
        val sortedNids = nids.toArray.sorted
        if (sortedNids.nonEmpty) {
          val cutDepth1 = math.pow(2, param.maxDepth/2)
          val cutDepth2 = math.pow(2, param.maxDepth*3/4)
          val start = sortedNids(0)
          if ((start >= cutDepth1 - 1 && start < cutDepth1 + cutDepth1 - 1) ||
            (start >= cutDepth2 - 1 && start < cutDepth2 + cutDepth2 - 1)) {
            (1 until sortedNids.length).foreach{ i =>
              activeNodes += sortedNids(i)
            }
            sortedNids.dropWhile(_!= null)
            nids.clear()
            nids += start
          }
        }
      } else {
        nids += activeNodes(0)
        activeNodes.remove(0)
      }

      val gatheredSplits = nids.toArray.map{nid =>
        (nid, bestOwnerIds(nid), bestAliasFids(nid), bestSplits(nid))}
      val validSplits = gatheredSplits.filter(_._4.isValid(param.minSplitVariance))
      val leaves = gatheredSplits.filter(!_._4.isValid(param.minSplitVariance)).map(_._1)
      if (gatheredSplits.nonEmpty) {
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
        hasActive = workers.map(_.splitNodes(bcValidSplits.value, bcSplitResults.value)).collect()(0)
        bcSplitResults.destroy()
        bcValidSplits.destroy()
        LogHelper.print(s"Split nodes cost ${System.currentTimeMillis() - splitStart} ms")
      } else {
        // no active nodes
        hasActive = false
      }
    }

    // 3. finish tree

    workers.foreach(worker => {
      worker.finishTree()
    })

    bcFidToGroupId.destroy()
    bcFidToNewFid.destroy()
    bcGroupSizes.destroy()


    // TODO: check equality
    val groupIdToFid = bcGroupIdToFid.value
    val forest = workers.map{ worker => worker.getTree()}.collect()(0)
    forest.zipWithIndex.foreach {
      case (tree, treeId) =>
        println(s"Tree[${treeId + 1}] contains ${tree.size} nodes " +
          s"(${(tree.size - 1) / 2 + 1} leaves)")
    }


    val instanceInfo = workers.map{ worker =>
      if (worker.workerId == 0) worker.getInstanceInfo()
      else null
    }.filter(_!= null).collect()(0)

    val nodes = forest(0).getNodes
    val nodeIds = forest(0).getNodes.keySet()
    var count =0
    (0 until Maths.pow(2, param.maxDepth + 1) - 1).foreach{ nid =>
      if (nodeIds.contains(nid) && !(nodeIds.contains(2 * nid + 1) || nodeIds.contains(2 * nid + 2))) {
        if (nodes.get(nid).isLeaf && instanceInfo.getNodePosEnd(nid) - instanceInfo.getNodePosStart(nid) + 1 != 0) {
          count += 1
          println(s"leaf: ${nid}, ${instanceInfo.getNodePosEnd(nid) - instanceInfo.getNodePosStart(nid) + 1}")
        }
      }
    }

    println(s"number of not non leaf: ${count}")


    PSContext.getOrCreate(sc)
    val index = sc.parallelize((0 until numTrain).map(_.toLong))
    val model = KDTreePSModel(0, numTrain, index, numPsPartitions)//todo???

    workers.foreach{ worker => worker.getTreeData(model, groupIdToFid(worker.workerId), param.numFeature, batchSize)}

    var epoch = numTrain / batchSize
    if (numTrain % batchSize != 0) epoch += 1

    val labels = labelsRDD.collect()
    labelsRDD.unpersist()
    val res = sc.parallelize(0 until epoch).flatMap { i =>
      val size = if ((i + 1) * batchSize <= numTrain) batchSize else numTrain % batchSize
      val pairs = model.getFeatures((i * batchSize until i * batchSize + size).map(i => instanceInfo.nodeToIns(i).toLong).toArray)
      val it = pairs.long2ObjectEntrySet().fastIterator()
      val insIds = new ArrayBuffer[Long]()
      val feats = new ArrayBuffer[IntFloatVector]()
      while (it.hasNext) {
        val entry = it.next()
        val (node, f) = (entry.getLongKey, entry.getValue)
        insIds.append(labels(node.toInt))
        feats.append(f)
      }
      Iterator((insIds.toArray, feats.toArray))
    }

    (forest, instanceInfo, res, labels)
  }

  def save(model: Seq[KDTree], instanceInfo: InstanceInfo, data: RDD[(Array[Long], Array[IntFloatVector])], labels: Array[Int], basePath: String)(implicit sc: SparkContext): Unit = {
    val path = new Path(basePath)
    val fs = path.getFileSystem(sc.hadoopConfiguration)
    if (fs.exists(path)) fs.delete(path, true)

    val modelPath = basePath + "/model"
    sc.parallelize(Seq(model)).saveAsObjectFile(modelPath)
    val instanceInfoPath = basePath + "/instanceInfo"
    sc.parallelize(Seq(instanceInfo)).saveAsObjectFile(instanceInfoPath)
    val datasetPath = basePath + "/dataset"
    data.saveAsObjectFile(datasetPath)
    val labelPath = basePath + "/label"
    sc.parallelize(labels).saveAsObjectFile(labelPath)
    PSContext.stop()
  }

  def mergeArray(nums1: Array[(Int, Float)], nums2: Array[(Int, Float)]): Array[(Int, Float)] = {
    Array.concat(nums1, nums2).sortBy(f => f._1)
  }


}


object KDTTrainer {

  def featureInfoOfGroup(featureInfo: FeatureInfo, groupId: Int,
                         groupIdToFid: Array[Int]): FeatureInfo = {
    val groupSize = groupIdToFid.length
    val featTypes = new Array[Boolean](groupSize)
    val numBin = new Array[Int](groupSize)
    val splits = new Array[Array[Float]](groupSize)
    val counts = new Array[Array[Int]](groupSize)
    val defaultBins = new Array[Int](groupSize)
    groupIdToFid.view.zipWithIndex.foreach {
      case (fid, newFid) =>
        featTypes(newFid) = featureInfo.isCategorical(fid)
        numBin(newFid) = featureInfo.getNumBin(fid)
        splits(newFid) = featureInfo.getSplits(fid)
        counts(newFid) = featureInfo.getCounts(fid)
        defaultBins(newFid) = featureInfo.getDefaultBin(fid)
    }
    new FeatureInfo(featTypes, numBin, splits, defaultBins, counts)
  }

  def balancedFeatureGrouping(featNNZ: Array[Int], numGroup: Int): (Array[Int], Array[Array[Int]], Array[Int], Array[Int]) = {
    val numFeature = featNNZ.length
    val fidToGroupId = new Array[Int](numFeature)
    val groupSizes = new Array[Int](numGroup)
    val groupNNZ = new Array[Long](numGroup)
    val sortedFeatNNZ = featNNZ.zipWithIndex.sortBy(_._1)
    for (i <- 0 until numFeature) {
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


}