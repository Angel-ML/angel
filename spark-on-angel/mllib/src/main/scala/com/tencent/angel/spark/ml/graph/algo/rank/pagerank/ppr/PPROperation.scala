package com.tencent.angel.spark.ml.graph.algo.rank.pagerank.ppr

import com.tencent.angel.graph.common.psf.result.GetLongsResult
import com.tencent.angel.spark.ml.psf.neighbors.DynamicSimple.{SampleWithCount, SampleWithTag, SampleWithTagParam, SampleWithTagResult, SampleWithTagByNodeIdResult, SampleWithTagByNodeId}
import com.tencent.angel.spark.ml.psf.neighbors.SampleNeighborsWithCount.GetNeighborWithCountParam
import com.tencent.angel.spark.models.PSMatrix
import it.unimi.dsi.fastutil.longs.{Long2IntOpenHashMap, Long2ObjectOpenHashMap}
import it.unimi.dsi.fastutil.objects.{Object2ObjectOpenHashMap}
import java.util.{HashMap => JHashMap}
import scala.collection.mutable.ArrayBuffer
import scala.util.Random

object PPROperation {

  def firstStepSampling(psMatrix: PSMatrix, nodeIds: Array[Long], count: Array[Int]): Long2ObjectOpenHashMap[Array[Long]] =
    psMatrix.psfGet(new SampleWithCount(new GetNeighborWithCountParam(psMatrix.id, nodeIds, count)))
    .asInstanceOf[GetLongsResult].getData

  def getSampledNeighbors(psMatrix: PSMatrix, nodeIds: Array[Long], tags: Array[Int]): SampleWithTagResult =
    psMatrix.psfGet(new SampleWithTag(new SampleWithTagParam(psMatrix.id, nodeIds, tags)))
    .asInstanceOf[SampleWithTagResult]

  def getSampledNeighborsWithNodeId(psMatrix: PSMatrix, nodeIds: Array[Long], tags: Array[Int]): SampleWithTagByNodeIdResult =
    psMatrix.psfGet(new SampleWithTagByNodeId(new SampleWithTagParam(psMatrix.id, nodeIds, tags)))
      .asInstanceOf[SampleWithTagByNodeIdResult]

  def sampleByBatch(psMatrix: PSMatrix, index: Int, seedsIter: Iterator[Long], batchSize: Int, numWalks: Int=10,
                    stopProb: Double, maxLength: Int, topk: Int): Iterator[(Long, String)] = {
    val r = new Random()
    var startTs = System.currentTimeMillis()
    var psTime = 0L
    var calcTime = 0L
    var counterTime = 0L
    val splitIndex = (numWalks * stopProb).toInt
    seedsIter.sliding(batchSize, batchSize).zipWithIndex.flatMap { case (seeds_, batchIndex) =>
      println(s"part $index: last batch cost ${System.currentTimeMillis() - startTs} ms, calc cost ${System.currentTimeMillis() - calcTime} ms, " +
        s"psTime $psTime ms, counterTime $counterTime ms.")
      startTs = System.currentTimeMillis()
      // first sampling
      psTime = 0L
      counterTime = 0L
      val seeds = seeds_.toArray
      val counter = seeds.map(_ => new Long2IntOpenHashMap(numWalks))
      var startT = System.currentTimeMillis()
      val first = firstStepSampling(psMatrix, seeds, seeds.map(_ => numWalks))
      var sampleT = System.currentTimeMillis() - startT
      psTime += sampleT
      println(s"part $index, batch $batchIndex, first sampling time: $sampleT ms.")
      var counter_ = new ArrayBuffer[(Long, Int)]()
      seeds.indices.foreach { idx =>
        val sam = first.get(seeds(idx))
        startT = System.currentTimeMillis()
        if (sam != null && sam.nonEmpty) {
          sam.slice(0, splitIndex+1).foreach { x => counter(idx).addTo(x, 1)}
          counterTime += (System.currentTimeMillis() - startT)
          counter_ ++= sam.slice(splitIndex+1, sam.length).map(x => (x, idx))
        }
      }
      var length = 1
      var stopSampling = counter_.isEmpty
      while (!stopSampling) {
        startT = System.currentTimeMillis()
        val (nodes2Sample, idxs) = counter_.unzip
        startT = System.currentTimeMillis()
        val sampled = getSampledNeighbors(psMatrix, nodes2Sample.toArray, idxs.toArray)
        val sampledNodes = sampled.getNodeIds
        val tags = sampled.getTags
        sampleT = System.currentTimeMillis() - startT
        println(s"part $index, batch $batchIndex, ${length}th, numKeys: ${nodes2Sample.length}, sampling time: $sampleT ms.")
        psTime += sampleT
        if (length < maxLength) {
          startT = System.currentTimeMillis()
          counter_.clear()
          for (elem <- sampledNodes.indices) {
            val d = r.nextFloat()
              // if the path is to be killed or has no next sampling node, kill it
            if (d < stopProb) {
              counter(tags(elem)).addTo(sampledNodes(elem), 1)
            } else counter_.append((sampledNodes(elem), tags(elem)))
          }
          counterTime += (System.currentTimeMillis() - startT)
          stopSampling = counter_.isEmpty
        } else {
          startT = System.currentTimeMillis()
          sampledNodes.zip(tags).foreach { case (node, idx) => counter(idx).addTo(node, 1)}
          counterTime += (System.currentTimeMillis() - startT)
          stopSampling = true
        }
        length += 1
      }
      calcTime = System.currentTimeMillis()
      counter.indices.map { idx =>
        val count = counter(idx)
        val ppr = if (count.isEmpty) "" else
          count.keySet().toLongArray()
          .map(x => (x, count.get(x)))
          .sortWith(_._2 > _._2).take(topk)
          .map(x => x._1 + ":" + x._2.toFloat / numWalks)
          .mkString(" ")
        (seeds(idx), ppr)
      }
    }
  }

  def sampleByBatchWithPath(psMatrix: PSMatrix, index: Int, seedsIter: Iterator[Long], batchSize: Int, numWalks: Int=10,
                            stopProb: Double, maxLength: Int, topk: Int): Iterator[(Long, String)] = {
    val r = new Random()
    var startTs = System.currentTimeMillis()
    var psTime = 0L
    var calcTime = 0L
    var counterTime = 0L
    val splitIndex = (numWalks * stopProb).toInt
    seedsIter.sliding(batchSize, batchSize).zipWithIndex.flatMap { case (seeds_, batchIndex) =>
      println(s"part $index: last batch cost ${System.currentTimeMillis() - startTs} ms, calc cost ${System.currentTimeMillis() - calcTime} ms, " +
        s"psTime $psTime ms, counterTime $counterTime ms.")
      startTs = System.currentTimeMillis()
      // first sampling
      psTime = 0L
      counterTime = 0L
      val seeds = seeds_.toArray
      val counter = seeds.map(_ => new Long2IntOpenHashMap(numWalks))
      val path = seeds.map(_ => new JHashMap[Long, Array[Long]]())
      var startT = System.currentTimeMillis()
      val first = firstStepSampling(psMatrix, seeds, seeds.map(_ => numWalks))
      var sampleT = System.currentTimeMillis() - startT
      psTime += sampleT
      println(s"part $index, batch $batchIndex, first sampling time: $sampleT ms.")
      var counter_ = new ArrayBuffer[(Long, Int, Array[Long])]()
      seeds.indices.foreach { idx =>
        val sam = first.get(seeds(idx))
        startT = System.currentTimeMillis()
        if (sam != null && sam.nonEmpty) {
          sam.slice(0, splitIndex+1).foreach ( x => {
            counter(idx).addTo(x, 1)
            path(idx).put(x, Array[Long](seeds(idx), x))
          })
          counterTime += (System.currentTimeMillis() - startT)
          counter_ ++= sam.slice(splitIndex+1, sam.length).map(x => (x, idx, Array[Long](seeds(idx), x)))
        }
      }
      var length = 1
      var stopSampling = counter_.isEmpty
      while (!stopSampling) {
        startT = System.currentTimeMillis()
        val (nodes2Sample, idxs, path_) = counter_.unzip3
        val nodes2PathMap = new Object2ObjectOpenHashMap[(Long, Int), Array[Long]](nodes2Sample.zip(idxs).toArray, path_.toArray)
        startT = System.currentTimeMillis()
        val sampled = getSampledNeighborsWithNodeId(psMatrix, nodes2Sample.toArray, idxs.toArray)
        val sourceNodes = sampled.getNodeIds
        val sampledNodes = sampled.getNeighbors
        val tags = sampled.getTags
        sampleT = System.currentTimeMillis() - startT
        println(s"part $index, batch $batchIndex, ${length}th, numKeys: ${nodes2Sample.length}, sampling time: $sampleT ms.")
        psTime += sampleT
        if (length < maxLength) {
          startT = System.currentTimeMillis()
          counter_.clear()
          for (elem <- sampledNodes.indices) {
            val d = r.nextFloat()
            // if the path is to be killed or has no next sampling node, kill it
            if (d < stopProb) {
              counter(tags(elem)).addTo(sampledNodes(elem), 1)
              if (counter(tags(elem)).get(sampledNodes(elem)) == 1) {
                path(tags(elem)).put(sampledNodes(elem), nodes2PathMap.get((sourceNodes(elem), tags(elem))) ++ Array[Long](sampledNodes(elem)))
              }
            } else counter_.append((sampledNodes(elem), tags(elem), nodes2PathMap.get((sourceNodes(elem), tags(elem))) ++ Array[Long](sampledNodes(elem))))
          }
          counterTime += (System.currentTimeMillis() - startT)
          stopSampling = counter_.isEmpty
        } else {
          startT = System.currentTimeMillis()
          sourceNodes.zip(sampledNodes.zip(tags)).foreach { case (source, (neighbor, idx)) =>
            counter(idx).addTo(neighbor, 1)
            if (counter(idx).get(neighbor) == 1) {
              path(idx).put(neighbor, nodes2PathMap.get((source, idx)) ++ Array[Long](neighbor))
            }
          }
          counterTime += (System.currentTimeMillis() - startT)
          stopSampling = true
        }
        length += 1
      }
      calcTime = System.currentTimeMillis()
      counter.indices.map { idx =>
        val count = counter(idx)
        val one_path = path(idx)
        val ppr = if (count.isEmpty) "" else
          count.keySet().toLongArray()
            .map(x => (x, count.get(x), one_path.get(x)))
            .sortWith(_._2 > _._2).take(topk)
            .map(x => x._1 + ":" + x._2.toFloat / numWalks + ":" + x._3.mkString("-"))
            .mkString(" ")
        (seeds(idx), ppr)
      }
    }
  }
}