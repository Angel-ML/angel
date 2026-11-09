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
package com.tencent.angel.spark.examples

import com.tencent.angel.conf.AngelConf
import com.tencent.angel.graph.client.psf.sample.sampleneighbor.SampleType
import com.tencent.angel.graph.data.{Feature, LongNeighbor}
import com.tencent.angel.ml.math2.storage.IntFloatDenseVectorStorage
import com.tencent.angel.ml.math2.vector.IntFloatVector
import com.tencent.angel.spark.context.PSContext
import com.tencent.angel.spark.ml.core.ArgsUtil
import com.tencent.angel.spark.ml.graph.GraphModel
import com.tencent.angel.spark.ml.graph.data.VertexId
import org.apache.spark.{SparkConf, SparkContext}

object GraphModelTest {


  def main(args: Array[String]): Unit = {
    val params = ArgsUtil.parse(args)
    val mode = params.getOrElse("mode", "local")
    val sc = start(mode)
    val input = params.getOrElse("input", "data/graph/graph-test")

    // example testing data, "nodeId  neighbors nodeTypes edgeTypes weights labels  nodeFeat  edgeFeats"
    //    1	1,2,3	0,1,2	2,1,0	0.1,0.3,0.2	1,1,0	0.1,0.1	0.1,0.1 0.2,0.2
    //    2	2,2,3	0,2,2	2,2,0	0.2,0.3,0.2	1,1,0	0.2,0.2	0.2,0.2 0.2,0.2
    //    3	3,2,3	0,3,2	2,3,0	0.3,0.3,0.2	1,1,0	0.3,0.3	0.3,0.3 0.2,0.2
    //    4	4,2,3	0,4,2	2,4,0	0.4,0.3,0.2	1,1,0	0.4,0.4	0.4,0.4 0.2,0.2
    //    5	5,2,3	0,5,2	2,5,0	0.5,0.3,0.2	1,1,0	0.5,0.5	0.5,0.5 0.2,0.2

    val data = sc.textFile(input).map(x => x.split("\t"))
    val neighbors = data.map(x => (x(0).toLong, x(1).split(",").map(_.toLong)))
    val nodeTypes = data.map(x => (x(0).toLong, x(2).split(",").map(_.toInt)))
    val edgeTypes = data.map(x => (x(0).toLong, x(3).split(",").map(_.toInt)))
    val weights = data.map(x => (x(0).toLong, x(4).split(",").map(_.toFloat)))
    val labels = data.map(x => (x(0).toLong, x(5).split(",").map(_.toFloat)))
    val nodeFeat = data.map { x =>
      val f = x(6).split(",").map(_.toFloat)
      (x(0).toLong, new Feature(f))
    }
    val edgeFeats = data.map { x =>
      val f = x(7).split(" ").map(_.split(",").map(_.toFloat)).map { x =>
        new Feature(x)
      }
      (x(0).toLong, f)
    }

    val maxIndex = neighbors.map(_._1).max() + 1
    val batchSize = 10
    val pullBatchSize = 10
    val psPartNum = 1
    val numModes = neighbors.map(_._1).count()
    val minIndex = neighbors.map(_._1).min()
    val graphModel = GraphModel(maxIndex, batchSize, pullBatchSize, psPartNum, numModes, minIndex)
    graphModel.createGraph()

    graphModel.initNeighbors(neighbors)
    graphModel.initNodeTypes(nodeTypes)
    graphModel.initEdgeTypes(edgeTypes)
    graphModel.initEdgeWeights(weights)
    graphModel.initLabels(labels)
    graphModel.initNodeFeatures(nodeFeat)
//    graphModel.initEdgeFeatures(edgeFeats)

    val nodes: Array[VertexId] = neighbors.map(_._1).collect()
//    val pullNbrs = graphModel.getNeighbors(nodes)
//    val pullNodeTypes = graphModel.getNodeTypes(nodes)
//    val pullEdgeTypes = graphModel.getEdgeTypes(nodes)
//    val pullWeights = graphModel.getEdgeWeights(nodes)
//    val pullLabels = graphModel.getLabels(nodes)
//    val pullNodeFeat = graphModel.getNodeFeats(nodes)
//    val pullEdgeFeats = graphModel.getEdgeFeats(nodes)

    println(s"----- check pulled results ------")


    println("----- sample neighbors ------")
    val sampleNeighbors = graphModel.sampleOps.sampleNeighbors(nodes, 5)
    nodes.foreach { node =>
      val ns = sampleNeighbors.get(node).asInstanceOf[LongNeighbor].getNodeIds
      print(s"node: ${node} 's neighbors: ")
      ns.foreach(x => print(s"${x}, "))
      println()
    }

    println("----- sample neighbors with neigh type------")
    val sampleNeighborsWithNeighType = graphModel.sampleOps.sampleNeighborsWithType(nodes, 5)
    nodes.foreach { node =>
      val ns = sampleNeighborsWithNeighType._1.get(node).asInstanceOf[LongNeighbor].getNodeIds
      val nsType = sampleNeighborsWithNeighType._2.get(node)
      print(s"node: ${node} 's neighbors: ")
      ns.zip(nsType).foreach(x => print(s"(${x._1}, ${x._2})"))
      println()
    }

    println("----- sample neighbors with edge type------")
    val sampleNeighborsWithEdgeType = graphModel.sampleOps.sampleNeighborsWithType(nodes, 5, SampleType.EDGE)
    nodes.foreach { node =>
      val ns = sampleNeighborsWithEdgeType._1.get(node).asInstanceOf[LongNeighbor].getNodeIds
      val nsType = sampleNeighborsWithEdgeType._3.get(node)
      print(s"node: ${node} 's neighbors: ")
      ns.zip(nsType).foreach(x => print(s"(${x._1}, ${x._2})"))
      println()
    }

    println("----- sample neighbors with both type------")
    val sampleNeighborsWithBothType = graphModel.sampleOps.sampleNeighborsWithType(nodes, 5, SampleType.NODE_AND_EDGE)
    nodes.foreach { node =>
      val ns = sampleNeighborsWithBothType._1.get(node).asInstanceOf[LongNeighbor].getNodeIds
      val nsType = sampleNeighborsWithBothType._2.get(node)
      val edgeType = sampleNeighborsWithBothType._3.get(node)
      print(s"node: ${node} 's neighbors: ")
      ns.zip(nsType).zip(edgeType).foreach(x => print(s"(${x._1._1}, ${x._1._2}, ${x._2})"))
      println()
    }

    println("----- sample neighbors with type with filter------")
    val sampleNeighborsWithType = graphModel.sampleOps.sampleNeighborsWithTypeWithFilter(nodes, Array(2L, 2L, 2L, 2L, 2L), Array(1L, 2L, 3L, 4L, 5L), 5)
    nodes.foreach { node =>
      val ns = sampleNeighborsWithType._1.get(node).asInstanceOf[LongNeighbor].getNodeIds
      val nsType = sampleNeighborsWithType._2.get(node)
      print(s"node: ${node} 's neighbors: ")
      ns.zip(nsType).foreach(x => print(s"(${x._1}, ${x._2})"))
      println()
    }

    println("----- sample neighbors with type with filter------")
    val sampleNeighborsWithType1 = graphModel.sampleOps.sampleNeighborsWithTypeWithFilter(nodes, Array(2L, 2L, 2L, 2L, 2L), null, 5)
    nodes.foreach { node =>
      val ns = sampleNeighborsWithType1._1.get(node).asInstanceOf[LongNeighbor].getNodeIds
      val nsType = sampleNeighborsWithType1._2.get(node)
      print(s"node: ${node} 's neighbors: ")
      ns.zip(nsType).foreach(x => print(s"(${x._1}, ${x._2})"))
      println()
    }

//    println("----- sample neighbors with type with filter------")
//    val sampleNeighborsWithType2 = graphModel.sampleOps.sampleNeighborsWithTypeWithFilter(nodes, null, Array(1L, 2L, 3L, 4L, 5L), 5)
//    nodes.foreach { node =>
//      val ns = sampleNeighborsWithType2._1.get(node)
//      val nsType = sampleNeighborsWithType2._2.get(node)
//      print(s"node: ${node} 's neighbors: ")
//      ns.zip(nsType).foreach(x => print(s"(${x._1}, ${x._2})"))
//      println()
//    }

    stop()
  }

  def start(mode: String = "local"): SparkContext = {
    val conf = new SparkConf()
    conf.setMaster(mode)
    conf.setAppName("hindex")
    conf.set(AngelConf.ANGEL_PSAGENT_UPDATE_SPLIT_ADAPTION_ENABLE, "false")
    val sc = new SparkContext(conf)
    sc.setLogLevel("WARN")
    //    sc.setCheckpointDir("cp")
    //    PSContext.getOrCreate(sc)
    sc
  }

  def stop(): Unit = {
    PSContext.stop()
    SparkContext.getOrCreate().stop()
  }
}
