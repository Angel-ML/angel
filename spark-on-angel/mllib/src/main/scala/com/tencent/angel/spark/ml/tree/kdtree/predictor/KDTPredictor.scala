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


package com.tencent.angel.spark.ml.tree.kdtree.predictor

import com.tencent.angel.ml.math2.storage.{IntFloatDenseVectorStorage, IntFloatSortedVectorStorage, IntFloatSparseVectorStorage}
import com.tencent.angel.ml.math2.vector.IntFloatVector
import com.tencent.angel.spark.ml.tree.data.Instance
import com.tencent.angel.spark.ml.tree.kdtree.metadata.InstanceInfo
import com.tencent.angel.spark.ml.tree.kdtree.tree.{KDTNode, KDTree}
import com.tencent.angel.spark.ml.tree.split.SplitPoint
import com.tencent.angel.spark.ml.tree.util.DataLoader
import it.unimi.dsi.fastutil.longs.Long2ObjectOpenHashMap
import org.apache.hadoop.fs.Path
import org.apache.spark.SparkContext
import org.apache.spark.ml.linalg.{Vector, Vectors}
import org.apache.spark.rdd.RDD

import scala.collection.mutable.ArrayBuffer

class KDTPredictor extends Serializable {

  var forest: Seq[KDTree] = _
  var instanceInfo: InstanceInfo = _
  var treeData: Map[Int, Vector] = _
  var labels: Array[Int] = _


  def loadModel(sc: SparkContext, modelPath: String): Unit = {
    forest = sc.objectFile[Seq[KDTree]](modelPath + "/model").collect().head
    println(s"Reading model from $modelPath")
    instanceInfo = sc.objectFile[InstanceInfo](modelPath + "/instanceInfo").collect().head
    val dataset = sc.objectFile[(Array[Long], Array[IntFloatVector])](modelPath + "/dataset")
    labels = sc.objectFile[Int](modelPath + "/label").collect()
    initTreeData(dataset)

    println(s"Reading instanceInfo from $modelPath")
  }

  def initTreeData(trainDP: RDD[(Array[Long], Array[IntFloatVector])]): Unit ={
    treeData = trainDP.flatMap{ batch =>
      batch._1.zip(batch._2).flatMap{pair =>
        Iterator((pair._1.toInt, makeFeature(pair._2)))}
    }.collect().toMap
  }

  def predict(predictor: KDTPredictor, instances: RDD[Instance], nearestK: Int, numPartitions: Int
             ): RDD[(Long, Vector, Int, Vector, Double)] = {
    val bcPredictor = instances.sparkContext.broadcast(predictor)
    val bcTreeData = instances.sparkContext.broadcast(treeData)
    treeData = null
    instances.repartition(numPartitions)
      .flatMap { instance =>
      val predRaw = bcPredictor.value.predictRaw(instance.feature, nearestK, bcTreeData.value)
      val predFeatures = bcPredictor.value.indexToFeature(instance.feature, predRaw, bcTreeData.value)
      predFeatures.flatMap(p => Iterator((instance.label.toLong, p._1, p._2, p._3, p._4)))
    }
  }

  def predict(implicit sc: SparkContext, nearestK: Int, numPartitions: Int, predictPath: String,
              outputPath: String, isSimple: Boolean, dataType: String, isGroupByCenter: Boolean = false): Unit = {
    val instances: RDD[Instance] = DataLoader.loadLibsvmDP(predictPath, forest.head.getParam.numFeature, dataType).cache()
    val preds = predict(this, instances, nearestK, numPartitions)
    instances.unpersist()
    preds.persist()

    val res = if (isGroupByCenter) {
      preds.groupBy(_._3).flatMap { pairs =>
        pairs._2.toArray.sortBy(_._5).flatMap { pair =>
          if (isSimple) {
            Iterator("seedId=" + pairs._1 + ", insId=" + pair._1 + ", distance=" + pair._5)
          } else {
            Iterator("seedId=" + pairs._1 + ", seed feature=" + pair._4 +
              ", insId=" + pair._1 + ", ins feature=" + pair._2 + ", distance=" + pair._5)
          }
        }
      }
    } else {
      preds.flatMap { pair =>
        if (isSimple) {
          Iterator("insId=" + pair._1 + ", seedId=" + pair._3 + ", distance=" + pair._5)
        } else {
          Iterator("insId=" + pair._1 + ", ins feature=" + pair._2 + ", seedId=" + pair._3 + ", seed feature=" + pair._4 +
            ", distance=" + pair._5)
        }
      }
    }
    preds.unpersist()


    val path = new Path(outputPath)
    val fs = path.getFileSystem(sc.hadoopConfiguration)
    if (fs.exists(path)) fs.delete(path, true)
    res.saveAsTextFile(outputPath)

    println(s"Writing predictions to $outputPath")
  }

  def getFeatures(ins: Array[Int], data: Map[Int, Vector]): Array[(Int, Vector)] = {
    ins.map { i =>
      (i, data.get(i).get)
    }
  }

  def getFeature(ins: Int, data: Map[Int, Vector]): (Int, Vector) = {
    (ins, data.get(ins).get)
  }

  def predictRaw(vec: Vector, nearestK: Int, data: Map[Int, Vector]): Array[(Int, Double)] = {
    val param = forest.head.getParam
    require(param.numFeature == vec.size, s"feature dimension should be ${param.numFeature}")
    val maxNodeNum = (Math.pow(2, param.maxDepth + 1) - 1).toInt
    var preds: Array[(Int, Double)] = null
    val nodeFlag = Array.fill(maxNodeNum)(true)
    val nodeDst = new ArrayBuffer[Double]()
    val nodeIdx = new ArrayBuffer[Int]()

    /*
     * calculate the num of data which larger than minNodeDst
     */
    def calcRest(K: Int, node: KDTNode): Int = {

      var rest = K

      val nodePosStart = instanceInfo.getNodePosStart(node.getNid)
      val nodePosEnd = instanceInfo.getNodePosEnd(node.getNid)
      val insIds = instanceInfo.nodeToIns

      //calculate the distance between vec and split node, and find the minNode
      val minNode = nodeDst.zipWithIndex.sortBy(f => f._1).filter(f => nodeFlag(f._2))
      if (minNode.isEmpty) return 0
      val (minNodeDst, _) = minNode(0)
      val insLabel = labels

      if ((nodePosStart <= nodePosEnd) && (nodePosStart != nodePosEnd || nodePosStart != 0)) {
        val dists = getFeatures((nodePosStart until nodePosEnd + 1).map { i => insLabel(insIds(i))}.toArray, data).map{v =>
          (v._1, distance(v._2, vec))}.sortBy(_._2)
        if (dists(0)._2 > minNodeDst) { // save as default value
          if (rest == nearestK && preds == null) {
            val len = if (dists.length > K) K else dists.length
            preds = Array.ofDim[(Int, Double)](len)
            System.arraycopy(dists, 0, preds, 0, len)
          } else {// merge two sorted array, select topK
          val res = merge(preds, dists, nearestK, minNodeDst)
            rest = res._1
            preds = res._2
          }
        } else if (dists.length >= K){ // select k
          if (dists(K - 1)._2 > minNodeDst) {
            if (rest == nearestK && preds == null) {
              val idx = dists.toMap.values.toArray.indexWhere({v: Double => v > minNodeDst})
              preds = Array.ofDim[(Int, Double)](nearestK)
              System.arraycopy(dists, 0, preds, 0, nearestK)
              rest = nearestK - idx
            } else { // merge two array, select topK
              val res = merge(preds, dists, nearestK, minNodeDst)
              rest = res._1
              preds = res._2
            }
          } else { // the num of data in node more than nearestK and all dist < minNodeDst
            if (rest == nearestK && preds == null) {
              preds = Array.ofDim[(Int, Double)](nearestK)
              System.arraycopy(dists, 0, preds, 0, nearestK)
              rest = 0
            } else { //
              val res = merge(preds, dists, nearestK, minNodeDst)
              rest = res._1
              preds = res._2
            }
          }
        } else if (dists.length < K) {
          if (dists(dists.length - 1)._2 > minNodeDst) {
            if (rest == nearestK && preds == null) {
              // the num of instances in node less than nearestK and not all dist < minNodeDst
              preds = Array.ofDim[(Int, Double)](dists.length)
              val idx = dists.toMap.values.toArray.indexWhere({v: Double => v > minNodeDst})
              System.arraycopy(dists, 0, preds, 0, dists.length)
              rest = nearestK - idx
            } else {
              val res = merge(preds, dists, nearestK, minNodeDst)
              rest = res._1
              preds = res._2
            }

          } else { // the num of instances in node less than nearestK and all dist < minNodeDst
            if (rest == nearestK && preds == null) {
              preds = Array.ofDim[(Int, Double)](dists.length)
              System.arraycopy(dists, 0, preds, 0, dists.length)
              rest = nearestK - dists.length
            } else {
              val res = merge(preds, dists, nearestK, minNodeDst)
              rest = res._1
              preds = res._2
            }
          }
        }
        nodeFlag(node.getNid) = false
        return rest

      } else {
        nodeFlag(node.getNid) = false
        return rest
      }
    }

    var node = getNearestLeafNode(forest.head.getRoot, vec, nodeDst, nodeIdx)

    var rest = calcRest(nearestK, node)

    //when the num of instances less than nearestK, floaw to other nodes to find more
    while(rest > 0 && node.getParent != null) {
      val parent = node.getParent
      if (node.equals(parent.getLeftChild) && parent.getRightChild != null && nodeFlag(parent.getRightChild.getNid)) {

        node = getNearestLeafNode(parent.getRightChild.asInstanceOf[KDTNode], vec, nodeDst, nodeIdx)

        rest = calcRest(rest, node)
      } else if (node.equals(parent.getRightChild) && parent.getLeftChild != null && nodeFlag(parent.getLeftChild.getNid)) {

        node = getNearestLeafNode(parent.getLeftChild.asInstanceOf[KDTNode], vec, nodeDst, nodeIdx)

        rest = calcRest(rest, node)

      } else if (parent.getRightChild == null || !nodeFlag(parent.getRightChild.getNid) ||
        parent.getLeftChild == null || !nodeFlag(parent.getLeftChild.getNid)) {
        node = parent.asInstanceOf[KDTNode]
        nodeFlag(node.getNid) = false
        val id = nodeIdx.indexOf(node.getNid)
        nodeDst.drop(id)
        nodeIdx.drop(id)
      }
    }

    preds
  }

  /*
   * assign vec to leaf node according to model rules
   */
  def getNearestLeafNode(root: KDTNode, vec: Vector, nodeDst: ArrayBuffer[Double], nodeIdx: ArrayBuffer[Int]): KDTNode ={
    var node = root
    while (!node.isLeaf) {
      val dst = node.getSplitEntry.asInstanceOf[SplitPoint].distance(vec)
      nodeDst.append(dst)
      nodeIdx.append(node.getNid)
      if (node.getSplitEntry.flowTo(vec) == 0)
        node = node.getLeftChild.asInstanceOf[KDTNode]
      else
        node = node.getRightChild.asInstanceOf[KDTNode]
    }
    node
  }

  /*
   * calculate the distance between two vectors
   */
  def distance(v1: Vector, v2: Vector): Double = {
    Math.sqrt(Vectors.sqdist(v1, v2))
  }

  /*
   * merge two sorted array, return sorted array and the num of values which larger than minNodeDst
   */
  def merge(nums1: Array[(Int, Double)], nums2: Array[(Int, Double)], nearestK: Int,
            minNodeDst: Double): (Int, Array[(Int, Double)]) = {
    val tmp = Array.concat(nums1, nums2).sortBy(f => f._2)
    var count = 0
    val len = if (tmp.length > nearestK) nearestK else tmp.length
    val preds = Array.ofDim[(Int, Double)](len)
    (0 until len).map{ i =>
      if (tmp(i)._2 <= minNodeDst) count += 1
      preds(i) = tmp(i)
    }
    (nearestK - count, preds)
  }

  /*
   * use insId to to associate feature
   */
  def indexToFeature(vec: Vector, preds: Array[(Int, Double)], data: Map[Int, Vector]): Array[(Vector, Int, Vector, Double)] = {
    getFeatures(preds.map(_._1), data).zip(preds).map{ case (feat, pred) =>
      (vec, pred._1, feat._2, pred._2)
    }
  }

  def predictRaw(indices: Array[Int], values: Array[Double], nearestK: Int, data: Map[Int, Vector]): Array[(Int, Double)] = {
    val param = forest.head.getParam
    require(param.numFeature > indices.last, s"feature dimension should be ${param.numFeature}")
    val vec = Vectors.sparse(param.numFeature, indices, values)
    predictRaw(vec, nearestK, data)
  }

  def predictRaw(features: Array[Double], nearestK: Int, data: Map[Int, Vector]): Array[(Int, Double)] = {
    val param = forest.head.getParam
    require(param.numFeature >= features.length, s"feature dimension should be ${param.numFeature}")
    val vec = Vectors.dense(features)
    predictRaw(vec, nearestK, data)
  }

  def predict(vec: Vector, nearestK: Int, data: Map[Int, Vector]): Array[(Vector, Int, Vector, Double)]  = {
    val param = forest.head.getParam
    require(param.numFeature == vec.size, s"feature dimension should be ${param.numFeature}")
    val preds = predictRaw(vec, nearestK, data)
    indexToFeature(vec, preds, data)
  }

  def predict(features: Array[Double], nearestK: Int, data: Map[Int, Vector]): Array[(Vector, Int, Vector, Double)]  = {
    val param = forest.head.getParam
    require(param.numFeature >= features.length, s"feature dimension should be ${param.numFeature}")
    val vec = Vectors.dense(features)
    val preds = predictRaw(vec, nearestK, data)
    indexToFeature(vec, preds, data)
  }

  def predict(indices: Array[Int], values: Array[Double], nearestK: Int, data: Map[Int, Vector]): Array[(Vector, Int, Vector, Double)] = {
    val param = forest.head.getParam
    require(param.numFeature > indices.last, s"feature dimension should be ${param.numFeature}")
    val vec = Vectors.sparse(param.numFeature, indices, values)
    val preds = predictRaw(vec, nearestK, data)
    indexToFeature(vec, preds, data)
  }

  def makeFeatures(features: Long2ObjectOpenHashMap[IntFloatVector]): Array[(Int,Vector)] = {
    val size = features.size()
    val feats = new Array[(Int, Vector)](size)
    val it = features.long2ObjectEntrySet().fastIterator()
    var i = 0
    while (it.hasNext) {
      val entry = it.next()
      val (insId, f) = (entry.getLongKey, entry.getValue)
      feats(i) = (insId.toInt, makeFeature(f))
      i += 1
    }
    feats
  }

  def makeFeature(f: IntFloatVector): Vector = {
    f.getStorage match {
      case sorted: IntFloatSortedVectorStorage =>
        val indices = sorted.getIndices
        val values = sorted.getValues.map(_.toDouble)
        Vectors.sparse(f.getDim, indices, values)
      case sparse: IntFloatSparseVectorStorage =>
        val indices = sparse.getIndices
        val values = sparse.getValues.map(_.toDouble)
        indices.zip(values).sortBy(_._1).zipWithIndex.foreach{ case (vec, idx) =>
          indices(idx) = vec._1
          values(idx) = vec._2
        }
        Vectors.sparse(f.getDim, indices, values)
      case dense: IntFloatDenseVectorStorage =>
        val values = dense.getValues.map(_.toDouble)
        Vectors.dense(values)
    }
  }
}