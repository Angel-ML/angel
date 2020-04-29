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
package com.tencent.angel.spark.ml.graph.connectedcomponent.scc

import com.tencent.angel.ml.math2.VFactory
import com.tencent.angel.ml.math2.vector.{LongIntVector, LongLongVector}
import it.unimi.dsi.fastutil.booleans.BooleanArrayList
import it.unimi.dsi.fastutil.ints.IntArrayList
import it.unimi.dsi.fastutil.longs.LongArrayList

class SCCGraphPartition(index: Int,
                        keys: Array[Long],
                        srcFlags: Array[Boolean],
                        dstFlags: Array[Boolean],
                        neighbors: Array[Long],
                        srcNbrFlags: Array[Boolean],
                        dstNbrFlags: Array[Boolean],
                        indptr: Array[Int],
                        colorLabels: Array[Long],
                        indices: Array[Long]) extends Serializable {
  def initMsgs(model: SCCPSModel): Int = {
    val cMsgs = VFactory.sparseLongKeyLongVector(model.colorPSModel.dim)
    val tMsgs = VFactory.sparseLongKeyIntVector(model.tagPSModel.dim)
    for (i <- keys.indices){
      cMsgs.set(keys(i), keys(i))
      tMsgs.set(keys(i), 1)
      colorLabels(i) = 0
    }
    model.colorPSModel.initMsgs(cMsgs)
    model.tagPSModel.initMsgs(tMsgs)
    cMsgs.size().toInt + tMsgs.size().toInt
  }
  
  def dropSingle(model: SCCPSModel, numMsgs: Long): Long = {
    val inTagMsgs = model.tagPSModel.readAllMsgs()
    val outTagMsgs = VFactory.sparseLongKeyIntVector(inTagMsgs.dim())

    var dropCnt = 0L
    for (idx <- keys.indices) {
      if ((inTagMsgs.get(keys(idx)) == 1) && isSingle(idx, inTagMsgs)) {
        outTagMsgs.set(keys(idx), 0)
        colorLabels(idx) = keys(idx)
        dropCnt += 1
      }
    }
    model.tagPSModel.writeMsgs(outTagMsgs)
    dropCnt
  }
  
  def isSingle(nodeIdx: Int, inTagMsgs: LongIntVector): Boolean = {
    var retVal = false
    if ((!srcFlags(nodeIdx)) || (!dstFlags(nodeIdx)))
      retVal = true
    else {
      var j = indptr(nodeIdx)
      var bp = true // break point
      while (j < indptr(nodeIdx + 1) && bp) {
        // is src node but all nodes it refers to has been dropped
        if ((inTagMsgs.get(neighbors(j)) == 1) && srcNbrFlags(j)) {
          bp = false
        }
        j += 1
      }
      if (bp) {
        retVal = true
      }
      
      if (!retVal) {
        j = indptr(nodeIdx)
        bp = true
        // is dst node but all nodes that refers to it has been dropped
        while (j < indptr(nodeIdx + 1) && bp) {
          if ((inTagMsgs.get(neighbors(j)) == 1) && dstNbrFlags(j)) {
            bp = false
          }
          j += 1
        }
        if (bp) {
          retVal = true
        }
      }
      
    }
    retVal
  }
  
  // propagate the color so the scc is included in the nodes with same color
  def propagate(model: SCCPSModel, numMsgs: Long): Long = {
    val inColorMsgs = model.colorPSModel.readAllMsgs()
    val inTagMsgs = model.tagPSModel.readAllMsgs()

    val colorOutMsgs = VFactory.sparseLongKeyLongVector(inColorMsgs.dim())
    var changedNum = 0L
    for (idx <- keys.indices) {
      // node that remained in the graph
      if (inTagMsgs.get(keys(idx)) == 1) {
        var j = indptr(idx)
        var colorThis: Long = inColorMsgs.get(keys(idx))
        val prevColor = colorThis
        while (j < indptr(idx + 1)) {
          if (dstNbrFlags(j) && (inTagMsgs.get(neighbors(j)) == 1)) {
            val nbrColor = inColorMsgs.get(neighbors(j))
            // if ((nbrColor > 0) && nbrColor < colorThis)
            if (nbrColor < colorThis) {
              colorThis = nbrColor
            }
          }
          j += 1
    
        }
        if (colorThis < prevColor) {
          colorOutMsgs.set(keys(idx), colorThis)
          changedNum += 1
        }
      }
    }
    model.colorPSModel.writeMsgs(colorOutMsgs)
    changedNum
  }
  
  def process(model: SCCPSModel, numMsgs: Long): Long = {
    val inColorMsgs = model.colorPSModel.readAllMsgs()
    val inTagMsgs = model.tagPSModel.readAllMsgs()
    val outTagMsgs = inTagMsgs.clone()
    var finalNum = 0L
    for (idx <- keys.indices) {
      if (inTagMsgs.get(keys(idx)) == 1) {
        val colorThis = inColorMsgs.get(keys(idx))
        if (colorThis == keys(idx)) {
          outTagMsgs.set(keys(idx), 0)
          finalNum += 1
          colorLabels(idx) = colorThis
        } else {
          var j = indptr(idx)
          var bp = true
          while (j < indptr(idx + 1) && bp) {
            val nbrColor = inColorMsgs.get(neighbors(j))
            if ((nbrColor == colorThis) && srcNbrFlags(j) && (inTagMsgs.get(neighbors(j)) == 0)) {
              outTagMsgs.set(keys(idx), 0)
              finalNum += 1
              colorLabels(idx) = colorThis
              bp = false
            }
            j += 1
          }
        }
      }
    }
    
    model.tagPSModel.writeMsgs(outTagMsgs)
    finalNum
  }
  
  def repaint(model: SCCPSModel, colorRecord: LongLongVector) :Unit = {
    val curTag = model.tagPSModel.readAllMsgs()
    val curColor = model.colorPSModel.readAllMsgs()
    val repaintedColor = colorRecord.clone()
    
    for (idx <- keys.indices) {
      if (curTag.get(keys(idx)) == 0) {
        repaintedColor.set(keys(idx), curColor.get(keys(idx)))
      }
    }
    model.colorPSModel.writeMsgs(repaintedColor)
    
  }
  
  def save(): (Array[Long], Array[Long]) = {
    (keys, colorLabels)
  }
  
  
}

object SCCGraphPartition {
  def apply(index: Int, iterator: Iterator[(Long, Iterable[(Long, Boolean)])]): SCCGraphPartition = {
    val indptr = new IntArrayList()
    val keys = new LongArrayList()
    val neighbors = new LongArrayList()
    val srcNbrFlags = new BooleanArrayList()
    val dstNbrFlags = new BooleanArrayList()
    val srcFlags = new BooleanArrayList()
    val dstFlags = new BooleanArrayList()
    
    indptr.add(0)
    
    while (iterator.hasNext) {
      val (node, nts) = iterator.next()
      val (ns, ts) = nts.unzip
      keys.add(node)

      val bts = ts.toArray.distinct
      if (bts.length == 2){
        srcFlags.add(true)
        dstFlags.add(true)
      } else {
        if (bts(0)) {
          srcFlags.add(true)
          dstFlags.add(false)
        }
        else {
          srcFlags.add(false)
          dstFlags.add(true)
        }
      }

      nts.toArray.distinct.foreach(n => {
        neighbors.add(n._1)
        if (n._2) {
          srcNbrFlags.add(true)
          dstNbrFlags.add(false)
        } else {
          srcNbrFlags.add(false)
          dstNbrFlags.add(true)
        }
      })
      indptr.add(neighbors.size())
    }
    
    val keysArray = keys.toLongArray()
    val srcFlagsArray = srcFlags.toBooleanArray()
    val dstFlagsArray = dstFlags.toBooleanArray()
    val neighborsArray = neighbors.toLongArray()
    val srcNbrFlagsArray = srcNbrFlags.toBooleanArray()
    val dstNbrFlagsArray = dstNbrFlags.toBooleanArray()
    
    new SCCGraphPartition(index, keysArray, srcFlagsArray, dstFlagsArray,
      neighborsArray, srcNbrFlagsArray, dstNbrFlagsArray,
      indptr.toIntArray(), new Array[Long](keysArray.length), keysArray.union(neighborsArray).distinct)
  }
}
