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

package com.tencent.angel.graph.rank.kcore

import com.tencent.angel.graph.utils.BatchIter
import com.tencent.angel.graph.utils.collection.OpenHashMap
import com.tencent.angel.ml.math2.VFactory
import it.unimi.dsi.fastutil.ints.IntArrayList
import it.unimi.dsi.fastutil.longs.LongArrayList

import scala.collection.mutable

private[kcore]
class KCorePartition(index: Int,
                          keys: Array[Long],
                          indptr: Array[Int],
                          neighbors: Array[Long],
                          indexMap: OpenHashMap[Long, Int],
                          numStaticNeis: Array[Int] = null,
                          static2: Array[Long] = null,
                          threshold: Int = -1,
                          mode: String = "full") extends Serializable {
  
  final val denseMode: String = "dense"
  final val midMode: String = "mid"
  final val sparseMode: String = "sparse"
  final val fullMode: String = "full"
  
  def numNodes(): Int = keys.length
  
  def numEdges(): Int = neighbors.length
  
  def initCores(model: KCorePSModel, batchSize: Int = 100000): Unit = {
    keys.indices.sliding(batchSize, batchSize).foreach { iter =>
      val msgs = VFactory.sparseLongKeyIntVector(model.dim)
      if (this.mode == this.sparseMode || this.mode == this.midMode) {
        for (i <- iter) {
          println(s"init cores at i: ${indptr(i + 1) - indptr(i) + numStaticNeis(i)}")
          assert(indptr(i + 1) - indptr(i) + numStaticNeis(i) > 0)
          msgs.set(keys(i), indptr(i + 1) - indptr(i) + numStaticNeis(i))
        }
        
      }
      else {
        for (i <- iter)
          msgs.set(keys(i), indptr(i + 1) - indptr(i))
      }
      
      model.initCores(msgs)
      println(s"part $index, init ${msgs.size().toInt} cores.")
    }
  }
  
  def firstIterations(model: KCorePSModel, batchSize: Int = 10000, curIteration: Int, numFirsts: Int, preMinCore: Int = 0): (Int, Long) = {
    // first iteration, calc core for all nodes by batch
    var minCore = Int.MaxValue
    var batchIndex = 0
    var numMsgs = 0L
    keys.indices.sliding(batchSize, batchSize).foreach { iter =>
      val before = System.currentTimeMillis()
      val nbrs2pull = neighbors.slice(indptr(iter.head), indptr(iter.last + 1))
      val keys2pull = keys.slice(iter.head, iter.last + 1)
      val nodes2pull = nbrs2pull.union(keys2pull).distinct
      numMsgs += nodes2pull.length
      
      // pull cores
      val beforePull = System.currentTimeMillis()
      val pullCores = model.readCores(nodes2pull)
      val pullCoresTime = System.currentTimeMillis() - beforePull
      
      // start calculating
      val outMsgs = VFactory.sparseLongKeyIntVector(iter.size)
      val changed = VFactory.sparseLongKeyIntVector(pullCores.size())
      var keys2calc = 0
      for (idx <- iter) { // calc all keys for the first iteration
        val x = pullCores.get(keys(idx))
        if (x > preMinCore) {
          keys2calc += 1
          val neiCores = new IntArrayList()
          if (curIteration < numFirsts) {
            for (j <- indptr(idx) until indptr(idx + 1)) {
              val t = pullCores.get(neighbors(j))
              if (t > preMinCore) {
                neiCores.add(t)
              }
            }
            val neis = neiCores.toIntArray()
            val numStaticNbrs = if (this.mode == this.denseMode || this.mode == this.fullMode) 0 else numStaticNeis(idx)
            val newCore = if (neiCores.size + numStaticNbrs <= preMinCore) preMinCore else hindexWithPrior(neis, x, numStaticNbrs)
            if (newCore < x) {
              outMsgs.set(keys(idx), newCore)
              minCore = Math.min(minCore, newCore)
            }
          } else {
            val add = new LongArrayList()
            for (j <- indptr(idx) until indptr(idx + 1)) {
              val t = pullCores.get(neighbors(j))
              if (t > preMinCore) {
                neiCores.add(t)
                add.add(neighbors(j))
              }
            }
            val neis = neiCores.toIntArray()
            val numStaticNbrs = if (this.mode == this.denseMode || this.mode == this.fullMode) 0 else numStaticNeis(idx)
            val newCore = if (neiCores.size + numStaticNbrs <= preMinCore) preMinCore else hindexWithPrior(neis, x, numStaticNbrs)
            if (newCore < x) {
              add.toLongArray().foreach { n => changed.set(n, 1) }
              outMsgs.set(keys(idx), newCore)
              minCore = Math.min(minCore, newCore)
            }
          }
        }
      }
      if (curIteration == numFirsts) {
        model.writeMsgs(changed)
        numMsgs += changed.size()
      }
      model.updateCores(outMsgs)
      batchIndex += 1
      numMsgs += outMsgs.size()
      
      println(s"iter $curIteration, part $index, batch $batchIndex (${iter.length} keys), calc all keys, " +
        s"keys2calc vs keysChanged: $keys2calc vs ${outMsgs.size()}, " +
        s"pulling cost $pullCoresTime ms (${nodes2pull.length}), total cost ${System.currentTimeMillis() - before} ms.")
      outMsgs.clear()
      changed.clear()
    }
    (minCore, numMsgs)
  }
  
  def nextIterationsByBatch(model: KCorePSModel, batchSize: Int = 10000, curIteration: Int, preMinCore: Int = 0): (Int, Long) = {
    // first iteration, calc core for all nodes by batch
    var minCore = Int.MaxValue
    var batchIndex = 0
    var numMsgs = 0L
    val before = System.currentTimeMillis()
    assert(keys.length > 0)
    val keys2calc = model.readTag(keys.clone())
    numMsgs += keys2calc.length
    
    val pullKeys2calcTime = System.currentTimeMillis() - before
    if (keys2calc.nonEmpty) {
      keys2calc.sliding(batchSize, batchSize).foreach { batchKeys =>
        val before = System.currentTimeMillis()
        
        // check nbrs2pull and pull cores
        val nodes2pull = batchKeys.flatMap { key =>
          val idx = indexMap(key)
          neighbors.slice(indptr(idx), indptr(idx + 1))
        }.union(batchKeys).distinct
        numMsgs += nodes2pull.length
        
        val beforePull = System.currentTimeMillis()
        val pullCores = model.readCores(nodes2pull)
        val pullCoresTime = System.currentTimeMillis() - beforePull
        
        // start calculating
        val outMsgs = VFactory.sparseLongKeyIntVector(keys2calc.length)
        //        val changed = new LongArrayList()
        val changed = VFactory.sparseLongKeyIntVector(pullCores.size())
        batchKeys.foreach { key =>
          val idx = indexMap(key)
          val x = pullCores.get(keys(idx))
          val neiCores = new IntArrayList()
          val add = new LongArrayList()
          for (j <- indptr(idx) until indptr(idx + 1)) {
            val t = pullCores.get(neighbors(j))
            if (t > preMinCore) {
              neiCores.add(t)
              add.add(neighbors(j))
            }
          }
          val neis = neiCores.toIntArray()
          val numStaticNbrs = if (this.mode == this.denseMode || this.mode == this.fullMode) 0 else numStaticNeis(idx)
          //todo check for correctness
          val newCore =
            if ((neis.count(_ >= x) + numStaticNbrs) == x) x
            else if ((neiCores.size + numStaticNbrs) <= preMinCore) preMinCore
            else hindexWithPrior(neis, x, numStaticNbrs)
          
          assert(newCore > 0)
          
          if (newCore < x) {
            add.toLongArray().foreach { n => changed.set(n, 1) }
            outMsgs.set(keys(idx), newCore)
            minCore = Math.min(minCore, newCore)
          }
        }
        
        //        model.updateTag(changed.toLongArray)
        model.writeMsgs(changed.clone())
        model.updateCores(outMsgs.clone())
        numMsgs += outMsgs.size()
        numMsgs += changed.size()
        
        println(s"iter $curIteration, part $index(totalKeys2calc=${keys2calc.length}, pullKeys2calcTime=$pullKeys2calcTime), " +
          s"batch $batchIndex , keys2calc vs keysChanged: ${batchKeys.length} vs ${outMsgs.size()}, " +
          s"pullCoresTime=$pullCoresTime (${nodes2pull.length}), " +
          s"total cost ${System.currentTimeMillis() - before} ms.")
        batchIndex += 1
        outMsgs.clear()
        changed.clear()
      }
    } else {
      println(s"iter $curIteration, part $index(totalKeys2calc=${keys2calc.length}, pullKeys2calcTime=$pullKeys2calcTime)")
    }
    (minCore, numMsgs)
  }
  
  def compress(model: KCorePSModel, batchSize: Int = 10000): KCorePartition = {
    val newIndexMap = new OpenHashMap[Long, Int]()
    var seq = 0
    val newIndexptr = new IntArrayList()
    val newNeighbors = new LongArrayList()
    val newKeys = new LongArrayList()
    val newNumStaticNeis = new IntArrayList()
    newIndexptr.add(0)
    
    keys.indices.sliding(batchSize, batchSize).foreach { iter =>
      val nbrs2pull = neighbors.slice(indptr(iter.head), indptr(iter.last + 1))
      val keys2pull = keys.slice(iter.head, iter.last + 1)
      val nodes2pull = nbrs2pull.union(keys2pull).distinct
      val pullCores = model.readCores(nodes2pull)
      for (idx <- iter) {
        if (pullCores.get(keys(idx)) > threshold) {
          val newNeighbors_ = new LongArrayList()
          var j = indptr(idx)
          while (j < indptr(idx + 1)) {
            if (pullCores.get(neighbors(j)) > threshold)
              newNeighbors_.add(neighbors(j))
            j += 1
          }
          val numStaticNbrs = if (this.mode == this.denseMode) 0 else numStaticNeis(idx)
          if (newNeighbors_.size() + numStaticNbrs > threshold) {
            newNeighbors.addAll(newNeighbors_)
            newIndexptr.add(newNeighbors.size())
            newKeys.add(keys(idx))
            newIndexMap.update(keys(idx), seq)
            if (this.mode == this.midMode) {
              newNumStaticNeis.add(numStaticNbrs)
            }
            seq += 1
          }
        }
      }
    }
    if (this.mode == this.denseMode) {
      new KCorePartition(index, newKeys.toLongArray, newIndexptr.toIntArray,
        newNeighbors.toLongArray, newIndexMap, null, null, threshold, this.mode)
    }
    else {
      assert(this.mode == this.midMode, s"only dense-mode and mid-mode need to compress, please check mode")
      new KCorePartition(index, newKeys.toLongArray, newIndexptr.toIntArray,
        newNeighbors.toLongArray, newIndexMap, newNumStaticNeis.toIntArray(), null, threshold, this.mode)
    }
    
  }
  
  def save(model: KCorePSModel, batchSize: Int = 10000): Iterator[(Long, Int)] = {
    val allKeys = if (this.mode == this.sparseMode) keys ++ this.static2 else keys
    allKeys.sliding(batchSize, batchSize).flatMap { iter =>
      val pulled = model.readCores(iter)
      if (this.mode == this.fullMode || this.mode == this.midMode) {
        iter.map(x => (x, pulled.get(x)))
      }
      else {
        iter.flatMap { x =>
          if (pulled.get(x) > threshold) Iterator.single(x, pulled.get(x))
          else Iterator.empty
        }
      }
    }
  }
  
  
  def hindexWithPrior(arr: Array[Int], prior: Int, numStaticNbrs: Int = 0): Int = {
    assert(arr.length + numStaticNbrs != 0, s"calc h-index for empty array with 0 static neighbors.")
    val array = arr.sorted
    var cnt = prior
    var i = array.length - prior + numStaticNbrs
    if (prior > array.length) {
      cnt = array.length + numStaticNbrs
      i = 0
    }
    while ( i < array.length && array(i) < cnt) {
      i += 1
      cnt -= 1
    }
    cnt
  }
}


object KCorePartition {
  def applyFull(index: Int, iterator: Iterator[(Long, Array[Long])], batchSize: Int): KCorePartition = {
    val indexMap = new OpenHashMap[Long, Int]()
    var idx = 0
    val indptr = new IntArrayList()
    val keys = new LongArrayList()
    val neighbours = new LongArrayList()
    
    indptr.add(0)
    BatchIter(iterator, batchSize).foreach { batchIter =>
      batchIter.foreach { case (src, nbs) =>
        nbs.distinct.foreach(t => neighbours.add(t))
        indptr.add(neighbours.size())
        keys.add(src)
        indexMap.update(src, idx)
        idx += 1
      }
    }
    val keysArray = keys.toLongArray()
    val neighboursArray = neighbours.toLongArray()
    println(s"apply, part $index, num keys=${keysArray.length}, num edges=${neighboursArray.length}")
    new KCorePartition(index, keysArray, indptr.toIntArray(),
      neighboursArray, indexMap, null, null, -1, "full")
  }
  
  def applyDense(index: Int, iterator: Iterator[(Long, Array[Long])],
                 model: KCorePSModel, batchSize: Int, threshold: Int): KCorePartition = {
    val indexMap = new OpenHashMap[Long, Int]()
    var idx = 0
    val indptr = new IntArrayList()
    val keys = new LongArrayList()
    val neighbours = new LongArrayList()
    
    indptr.add(0)
    BatchIter(iterator, batchSize).foreach { batchIter =>
      val nodes2pull = new mutable.HashSet[Long]()
      nodes2pull ++= batchIter.flatMap(_._2)
      nodes2pull ++= batchIter.map(_._1)
      val beforePull = System.currentTimeMillis()
      val inMsgs = model.readMsgs(nodes2pull.toArray)
      println(s"apply, part $index, pull ${nodes2pull.size} msgs from ps, cost ${System.currentTimeMillis() - beforePull} ms.")
      batchIter.foreach { case (src, nbs) =>
        if (inMsgs.get(src) > 0) {
          val newNbs = new LongArrayList()
          nbs.foreach { nb => if (inMsgs.get(nb) > 0) newNbs.add(nb)}
          if (newNbs.size() > threshold) {
            neighbours.addAll(newNbs)
            indptr.add(neighbours.size())
            keys.add(src)
            indexMap.update(src, idx)
            idx += 1
          }
        }
      }
    }
    val keysArray = keys.toLongArray()
    val neighboursArray = neighbours.toLongArray()
    println(s"apply, part $index, num keys=${keysArray.length}, num edges=${neighboursArray.length}")
    new KCorePartition(index, keysArray, indptr.toIntArray(),
      neighboursArray, indexMap, null, null, threshold, "dense")
  }
  
  def applySparse(index: Int,iterator: Iterator[(Long, Array[Long], Int)],
                  batchSize: Int, threshold: Int): KCorePartition = {
    val indexMap = new OpenHashMap[Long, Int]()
    var idx = 0
    val indptr = new IntArrayList()
    val keys = new LongArrayList()
    val neighbours = new LongArrayList()
    val numStaticNeis = new IntArrayList()
    val static2 = new LongArrayList()
    
    indptr.add(0)
    while (iterator.hasNext) {
      val entry = iterator.next()
      val (node, ns, numStatic) = (entry._1, entry._2, entry._3)
      if (!(ns.isEmpty || (ns.length == 1 && numStatic == 0))) {
        ns.foreach { nei => neighbours.add(nei) }
        indptr.add(neighbours.size())
        keys.add(node)
        numStaticNeis.add(numStatic)
        indexMap.update(node, idx)
        idx += 1
      } else {
        static2.add(node)
      }
    }
    
    println(s"static2: ${static2.size()}")
    println(s"keys: ${keys.size()}")
    
    val keysArray = keys.toLongArray()
    val neighboursArray = neighbours.toLongArray()
    new KCorePartition(index, keysArray, indptr.toIntArray(),
      neighboursArray, indexMap, numStaticNeis.toIntArray(), static2.toLongArray(), -1, "sparse")
  }
  
  def applyMid(index: Int, iterator: Iterator[(Long, Array[Long], Int)],
               model: KCorePSModel, batchSize: Int, threshold: Int): KCorePartition = {
    val indexMap = new OpenHashMap[Long, Int]()
    var idx = 0
    val indptr = new IntArrayList()
    val keys = new LongArrayList()
    val neighbours = new LongArrayList()
    val numStaticNeis = new IntArrayList()
    
    indptr.add(0)
    BatchIter(iterator, batchSize).foreach{ batchIter =>
      val nodes2pull = new mutable.HashSet[Long]()
      nodes2pull ++= batchIter.flatMap(_._2)
      nodes2pull ++= batchIter.map(_._1)
      val inMsgs = model.readMsgs(nodes2pull.toArray)
      val beforePull = System.currentTimeMillis()
      println(s"apply, part $index, pull ${nodes2pull.size} msgs from ps, cost ${System.currentTimeMillis() - beforePull} ms.")
      batchIter.foreach{
        case (src, nbrs, numStatic) =>
          if (inMsgs.get(src) > 0) {
            val newNbs = new LongArrayList()
            nbrs.foreach{ nb =>
              if (inMsgs.get(nb) > 0) {
                newNbs.add(nb)
              }
            }
            if (newNbs.size() + numStatic > threshold) {
              neighbours.addAll(newNbs)
              indptr.add(neighbours.size())
              keys.add(src)
              numStaticNeis.add(numStatic)
              indexMap.update(src, idx)
              idx += 1
            }
          }
      }
    }
    
    val keysArray = keys.toLongArray()
    val neighboursArray = neighbours.toLongArray()
    new KCorePartition(index, keysArray, indptr.toIntArray(),
      neighboursArray, indexMap, numStaticNeis.toIntArray(), null, threshold, "mid")
  }
  
}
