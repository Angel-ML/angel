package com.tencent.angel.spark.ml.graph.ops

import com.tencent.angel.graph.client.psf.init.GeneralInitParam
import com.tencent.angel.graph.client.psf.init.initedgefeats.InitEdgeFeats
import com.tencent.angel.graph.client.psf.init.initedgetypes.InitEdgeTypes
import com.tencent.angel.graph.client.psf.init.initedgeweights.InitEdgeWeights
import com.tencent.angel.graph.client.psf.init.initlabels.InitLabels
import com.tencent.angel.graph.client.psf.init.initneighbors.InitNeighbor
import com.tencent.angel.graph.client.psf.init.initnodefeats.{InitNodeFeats, InitNodeFeatureParam}
import com.tencent.angel.graph.client.psf.init.initnodes.InitNodes
import com.tencent.angel.graph.client.psf.init.initnodetypes.InitNodeTypes
import com.tencent.angel.graph.data._
import com.tencent.angel.ml.math2.vector.IntFloatVector
import com.tencent.angel.ps.storage.vector.element.{ByteArrayElement, IElement}
import com.tencent.angel.spark.ml.graph.data.VertexId
import com.tencent.angel.spark.ml.graph.GraphModel
import com.tencent.angel.spark.models.PSMatrix
import com.twitter.chill.ScalaKryoInstantiator
import org.apache.spark.rdd.RDD

class InitOps(graph: GraphModel) extends Serializable {

  def initNodes(data: RDD[(VertexId, GraphNode)]): Unit = {
    data.mapPartitions { iter => {
      iter.sliding(graph.param.batchSize, graph.param.batchSize).map(pairs => initNodesByBatch(graph.graphMatrix, pairs))
    }
    }.count()
  }

  def initNeighbors(data: RDD[(VertexId, Array[VertexId])]): Unit = {
    data.mapPartitions { iter => {
      iter.sliding(graph.param.batchSize, graph.param.batchSize).map(pairs => initNeighborsByBatch(graph.graphMatrix, pairs))
    }
    }.count()
  }

  def initNodeFeatures(feats: RDD[(VertexId, Feature)]): Unit = {
    feats.mapPartitions { iter => {
      iter.sliding(graph.param.batchSize, graph.param.batchSize).map(pairs => initNodeFeaturesByBatch(graph.graphMatrix, pairs))
    }
    }.count()
  }

  def initEdgeFeatures(edgeFeats: RDD[(VertexId, EdgeFeats)]): Unit = {
    edgeFeats.mapPartitions { iter => {
      iter.sliding(graph.param.batchSize, graph.param.batchSize).map(pairs => initEdgeFeaturesByBatch(graph.graphMatrix, pairs))
    }
    }.count()
  }

  def initNodeTypes(nodeTypes: RDD[(VertexId, Array[Int])]): Unit = {
    nodeTypes.mapPartitions { iter => {
      iter.sliding(graph.param.batchSize, graph.param.batchSize).map(pairs => initNodeTypesByBatch(graph.graphMatrix, pairs))
    }
    }.count()
  }

  def initEdgeTypes(edgeTypes: RDD[(VertexId, Array[Int])]): Unit = {
    edgeTypes.mapPartitions { iter => {
      iter.sliding(graph.param.batchSize, graph.param.batchSize).map(pairs => initEdgeTypesByBatch(graph.graphMatrix, pairs))
    }
    }.count()
  }

  def initEdgeWeights(edgeWeights: RDD[(VertexId, Array[Float])]): Unit = {
    edgeWeights.mapPartitions { iter => {
      iter.sliding(graph.param.batchSize, graph.param.batchSize).map(pairs => initEdgeWeightsByBatch(graph.graphMatrix, pairs))
    }
    }.count()
  }

  def initLabels(labels: RDD[(VertexId, Array[Float])]): Unit = {
    labels.mapPartitions { iter => {
      iter.sliding(graph.param.batchSize, graph.param.batchSize).map(pairs => initLabelsByBatch(graph.graphMatrix, pairs))
    }
    }.count()
  }

  private def initNodesByBatch(graphMatrix: PSMatrix, pairs: Seq[(VertexId, GraphNode)]): Unit = {
    val nodeIds = new Array[VertexId](pairs.size)
    val nodes = new Array[IElement](pairs.size)

    pairs.zipWithIndex.foreach(elem => {
      nodeIds(elem._2) = elem._1._1
      nodes(elem._2) = elem._1._2
    })

    val func = new InitNodes(new GeneralInitParam(graphMatrix.id, nodeIds, nodes))
    graphMatrix.asyncPsfUpdate(func).get()
    println(s"init ${pairs.length} nodes")
  }


  private def initNeighborsByBatch(graphMatrix: PSMatrix, pairs: Seq[(VertexId, Array[VertexId])]): Unit = {
    val nodeIds = new Array[VertexId](pairs.size)
    val neightbors = new Array[IElement](pairs.size)

    pairs.zipWithIndex.foreach(elem => {
      nodeIds(elem._2) = elem._1._1
      neightbors(elem._2) = new LongNeighbor(elem._1._2)
    })

    val func = new InitNeighbor(new GeneralInitParam(graphMatrix.id, nodeIds, neightbors))
    graphMatrix.asyncPsfUpdate(func).get()
    println(s"init ${pairs.length} neighbors")
  }

  private def initNodeFeaturesByBatch(graphMatrix: PSMatrix, pairs: Seq[(VertexId, Feature)]): Unit = {
    val nodeIds = new Array[VertexId](pairs.size)
    val features = new Array[IElement](pairs.size)

    pairs.zipWithIndex.foreach(elem => {
      nodeIds(elem._2) = elem._1._1
      features(elem._2) = new ByteArrayElement(ScalaKryoInstantiator.defaultPool.toBytesWithoutClass(elem._1._2))
    })

    val func = new InitNodeFeats(new InitNodeFeatureParam(graphMatrix.id, nodeIds, features, FeatureKeyType.INT))
    graphMatrix.asyncPsfUpdate(func).get()
    println(s"init ${pairs.length} node features")
  }

  private def initEdgeFeaturesByBatch(graphMatrix: PSMatrix, pairs: Seq[(VertexId, EdgeFeats)]): Unit = {
    val nodeIds = new Array[VertexId](pairs.size)
    val features = new Array[IElement](pairs.size)

    pairs.zipWithIndex.foreach(elem => {
      nodeIds(elem._2) = elem._1._1
      features(elem._2) = elem._1._2
    })

    val func = new InitEdgeFeats(new GeneralInitParam(graphMatrix.id, nodeIds, features))
    graphMatrix.asyncPsfUpdate(func).get()
    println(s"init ${pairs.length} node edge features")
  }

  private def initNodeTypesByBatch(graphMatrix: PSMatrix, pairs: Seq[(VertexId, Array[Int])]): Unit = {
    val nodeIds = new Array[VertexId](pairs.size)
    val types = new Array[IElement](pairs.size)

    pairs.zipWithIndex.foreach(elem => {
      nodeIds(elem._2) = elem._1._1
      types(elem._2) = new NodeType(elem._1._2)
    })

    val func = new InitNodeTypes(new GeneralInitParam(graphMatrix.id, nodeIds, types))
    graphMatrix.asyncPsfUpdate(func).get()
    println(s"init ${pairs.length} node types")
  }

  private def initEdgeTypesByBatch(graphMatrix: PSMatrix, pairs: Seq[(VertexId, Array[Int])]): Unit = {
    val nodeIds = new Array[VertexId](pairs.size)
    val types = new Array[IElement](pairs.size)

    pairs.zipWithIndex.foreach(elem => {
      nodeIds(elem._2) = elem._1._1
      types(elem._2) = new EdgeType(elem._1._2)
    })

    val func = new InitEdgeTypes(new GeneralInitParam(graphMatrix.id, nodeIds, types))
    graphMatrix.asyncPsfUpdate(func).get()
    println(s"init ${pairs.length} edge types")
  }

  private def initEdgeWeightsByBatch(graphMatrix: PSMatrix, pairs: Seq[(VertexId, Array[Float])]): Unit = {
    val nodeIds = new Array[VertexId](pairs.size)
    val weights = new Array[IElement](pairs.size)

    pairs.zipWithIndex.foreach(elem => {
      nodeIds(elem._2) = elem._1._1
      weights(elem._2) = new Weights(elem._1._2)
    })

    val func = new InitEdgeWeights(new GeneralInitParam(graphMatrix.id, nodeIds, weights))
    graphMatrix.asyncPsfUpdate(func).get()
    println(s"init ${pairs.length} edge weights")
  }

  private def initLabelsByBatch(graphMatrix: PSMatrix, pairs: Seq[(VertexId, Array[Float])]): Unit = {
    val nodeIds = new Array[VertexId](pairs.size)
    val labels = new Array[IElement](pairs.size)

    pairs.zipWithIndex.foreach(elem => {
      nodeIds(elem._2) = elem._1._1
      labels(elem._2) = new Labels(elem._1._2)
    })

    val func = new InitLabels(new GeneralInitParam(graphMatrix.id, nodeIds, labels))
    graphMatrix.asyncPsfUpdate(func).get()
    println(s"init ${pairs.length} labels")
  }


}
