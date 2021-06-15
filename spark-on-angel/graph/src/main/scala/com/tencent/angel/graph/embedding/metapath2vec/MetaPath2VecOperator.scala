package com.tencent.angel.graph.embedding.metapath2vec

import com.carrotsearch.hppc.IntArrayList
import com.tencent.angel.graph.model.neighbor.simplewithtype.TypeNeighborElement
import com.tencent.angel.graph.utils.BatchIter
import com.tencent.angel.ps.storage.vector.element.IElement
import it.unimi.dsi.fastutil.longs.LongArrayList
import org.apache.spark.sql.Row

import scala.collection.mutable

object MetaPath2VecOperator {

  def initNeighborTableWithType(iter: Iterator[(Long, Array[Long])], batchSize: Int,
    model: MetaPath2VecPSModel) = {
    BatchIter(iter, batchSize).foreach { batchIter =>
      val start = System.currentTimeMillis()
      val nodes2pull = new mutable.HashSet[Long]()
      batchIter.foreach { case (src, nbrs) =>
        nodes2pull ++= nbrs
        nodes2pull.add(src)
      }
      val beforePull = System.currentTimeMillis()
      val types = model.readMsgs(nodes2pull.toArray)
      println(s"pull ${nodes2pull.size} nodes' type, cost ${System.currentTimeMillis() - beforePull} ms.")

      val nodeIds = new Array[Long](batchIter.length)
      val data = new Array[IElement](batchIter.length)
      for (idx <- batchIter.indices) {
        val (key, nbrs) = batchIter(idx)
        val (nbrs_, tags) = nbrs.map(x => (x, types.get(x))).sortWith(_._2 < _._2).unzip
        val indptr = new IntArrayList()
        val typeList = new IntArrayList()
        indptr.add(0)
        var tag =tags(0)
        typeList.add(tag)
        var i = 1
        while (i < nbrs_.length) {
          if (tag != tags(i)) {
            typeList.add(tags(i))
            indptr.add(i)
            tag = tags(i)
          }
          i += 1
        }
        indptr.add(nbrs_.length)
        nodeIds(idx) = key
        data(idx) = new TypeNeighborElement(types.get(key), nbrs_, typeList.toArray, indptr.toArray)
      }
      val beforePush = System.currentTimeMillis()
      model.initNeighbors(nodeIds, data)
      println(s"initNeighborTableWithType: last batch cost ${System.currentTimeMillis()-start} ms," +
        s"push ${batchIter.length} neighborTables to ps, cost ${System.currentTimeMillis()-beforePush} ms.")
    }
  }

  def initNeighborTable(iter: Iterator[(Long, Array[Long])], batchSize: Int,
                                model: MetaPath2VecPSModel) = {
    BatchIter(iter, batchSize).foreach { batchIter =>
      val start = System.currentTimeMillis()
      val nodeIds = new Array[Long](batchIter.length)
      val data = new Array[IElement](batchIter.length)
      for (idx <- batchIter.indices) {
        nodeIds(idx) = batchIter(idx)._1
        data(idx) = new TypeNeighborElement(batchIter(idx)._2)
      }
      val beforePush = System.currentTimeMillis()
      model.initNeighbors(nodeIds, data)
      println(s"initNeighborTable: last batch cost ${System.currentTimeMillis()-start} ms," +
        s"push ${batchIter.length} neighborTables to ps, cost ${System.currentTimeMillis()-beforePush} ms.")
    }
  }

  def sample(index: Int, iter: Iterator[(Long, Int)], thisPath: Array[Int],
             model: MetaPath2VecPSModel, batchSize: Int=100000) = {
    BatchIter(iter, batchSize).flatMap { batchIter =>
      val walkLen = thisPath.length + 1
      val result = batchIter.map(x => Array(x._1) ++ new Array[Long](thisPath.length))
      val endIdx = Array.fill(batchIter.length)(walkLen)
      var step = 0
      val sampleNodes = new LongArrayList()
      batchIter.foreach(x => sampleNodes.add(x._1))
      while (step < thisPath.length && (!sampleNodes.isEmpty)) {
        val sampleType = thisPath(step)
        val startTime = System.currentTimeMillis()
        val sampled = model.sample(sampleNodes.toLongArray, sampleType) // make sure the returned array is aligned with the sampleNodes
        val pullTime = System.currentTimeMillis() - startTime
        sampleNodes.clear()
        for (idx <- result.indices) {
          if (endIdx(idx) == walkLen) {
            val key = result(idx)(step)
            val next = sampled.get(key).next()
            if (next != key) {
              result(idx)(step+1) = next
              sampleNodes.add(next)
            } else {
              endIdx(idx) = step+1
            }
          }
        }
        println(s"step $step sampling: pulling and processing cost ${System.currentTimeMillis()-startTime} ms, " +
          s"pulling cost $pullTime ms.")
        step += 1
      }
      result.zip(endIdx).filter(_._2 > 1).map(x => Row(x._1.slice(0, x._2).mkString(" ")))
//      result.filter(_.length > 1).map(x => Row(x.mkString(" ")))
    }
  }
}