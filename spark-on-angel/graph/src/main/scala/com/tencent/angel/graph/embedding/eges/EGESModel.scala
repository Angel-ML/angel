package com.tencent.angel.graph.embedding.eges

import java.util.concurrent.TimeUnit

import com.tencent.angel.graph.common.param.ModelContext
import com.tencent.angel.graph.utils.ModelContextUtils
import com.tencent.angel.ml.core.optimizer.decayer.{StandardDecay, StepSizeScheduler}
import com.tencent.angel.ml.matrix.RowType
import com.tencent.angel.model.{MatrixLoadContext, MatrixSaveContext, ModelLoadContext, ModelSaveContext}
import com.tencent.angel.spark.context.PSContext
import com.tencent.angel.spark.ml.util.LogUtils
import com.tencent.angel.spark.models.PSMatrix
import com.tencent.angel.graph.embedding.line._
import com.tencent.angel.graph.embedding.FastSigmoid
import it.unimi.dsi.fastutil.ints.{Int2IntOpenHashMap, Int2ObjectOpenHashMap}
import org.apache.hadoop.fs.Path
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

import scala.collection.mutable.ArrayBuffer
import scala.util.Random

class EGESModel(numNode: Long,
                minIndex: Long,
                maxIndex: Long,
                embeddingDim: Int,
                numPart: Int,
                weightedSI: Boolean,
                numWeightsSI: Int,
                seed: Int = Random.nextInt,
                learningRate: Float,
                decayRate: Float) extends Serializable {

  val matrixName = "embedding"
  val modelContext = new ModelContext(numPart, minIndex, minIndex + numNode, numNode,
    matrixName, SparkContext.getOrCreate().hadoopConfiguration)
  val mc = if (modelContext.isUseHashPartition) {
    ModelContextUtils.createMatrixContext(modelContext, RowType.T_ANY_INTKEY_SPARSE,
      classOf[LINENode])
  } else {
    ModelContextUtils.createMatrixContext(modelContext, RowType.T_ANY_INTKEY_DENSE,
      classOf[LINENode])
  }
  val psMatrix = PSMatrix.matrix(mc)
  val matrixId = psMatrix.id

  val weightMatrixName = "weightSI"
  val numItemNode = maxIndex - minIndex + 1
  val weightModelContext = new ModelContext(numPart, minIndex, maxIndex + 1, numItemNode,
    weightMatrixName, SparkContext.getOrCreate().hadoopConfiguration)
  val mcWeight = if (weightModelContext.isUseHashPartition) {
    ModelContextUtils.createMatrixContext(weightModelContext, RowType.T_ANY_INTKEY_SPARSE,
      classOf[EmbeddingNode])
  } else {
    ModelContextUtils.createMatrixContext(weightModelContext, RowType.T_ANY_INTKEY_DENSE,
      classOf[EmbeddingNode])
  }
  val psWeightMatrix = PSMatrix.matrix(mcWeight)

  val ssScheduler: StepSizeScheduler = new StandardDecay(learningRate, decayRate)
  var totalWeightsPullTime: Long = 0
  var totalFeatsPullTime: Long = 0
  var totalWeightsPushTime: Long = 0
  var totalFeatsPushTime: Long = 0
  var totalMakeParamTime: Long = 0
  var totalCalTime: Long = 0
  var totalMakeGradTime: Long = 0
  var totalCallNum: Long = 0
  var totalWaitPullTime: Long = 0
  var totalWeightsNormTime: Long = 0

  // initialize weights and embeddings
  def randomInitialize(seed: Int): Unit = {
    var beforeRandomize = System.currentTimeMillis()
    if (modelContext.isUseHashPartition) {
      // Init as mini-batch
      modelContext.getMinNodeId.toInt.to(modelContext.getMaxNodeId.toInt).sliding(10000000, 10000000).foreach(e => {
        psMatrix.asyncPsfUpdate(new LINEModelRandomizeAsNodes(
          new RandomizeUpdateAsNodesParam(matrixId, embeddingDim, e.toArray, 2, seed)))
          .get(60000, TimeUnit.MILLISECONDS)
      })
      println(s"Embedding matrix successfully Randomized, cost ${(System.currentTimeMillis() - beforeRandomize) / 1000.0}s")
    } else {
      // Just init by range
      psMatrix.asyncPsfUpdate(new LINEModelRandomize(
        new RandomizeUpdateParam(matrixId, embeddingDim, 2, seed)))
        .get(1800000, TimeUnit.MILLISECONDS)
      println(s"Embedding matrix successfully Randomized, cost ${(System.currentTimeMillis() - beforeRandomize) / 1000.0}s")
    }

    beforeRandomize = System.currentTimeMillis()
    if (weightModelContext.isUseHashPartition) {
      // Init as mini-batch
      weightModelContext.getMinNodeId.toInt.to(weightModelContext.getMaxNodeId.toInt).sliding(10000000, 10000000).foreach(e => {
        psWeightMatrix.asyncPsfUpdate(new EGESModelRandomizeAsNodes(
          new WeightsRandomizeUpdateAsNodesParam(psWeightMatrix.id, numWeightsSI + 1, e.toArray, seed)))
          .get(60000, TimeUnit.MILLISECONDS)
      })
      println(s"Weight matrix successfully Randomized, cost ${(System.currentTimeMillis() - beforeRandomize) / 1000.0}s")
    } else {
      // Just init by range
      val beforeRandomize = System.currentTimeMillis()
      psWeightMatrix.asyncPsfUpdate(new EmbeddingModelRandomize(new WeightsRandomizeUpdateParam(psWeightMatrix.id,
        numWeightsSI + 1, seed, 1))).get(1800000, TimeUnit.MILLISECONDS)
      println(s"Weight matrix successfully Randomized, cost ${(System.currentTimeMillis() - beforeRandomize) / 1000.0}s")
    }
  }

  def this(params: EGESParam) {
    this(params.numNode, params.minIndex, params.maxIndex, params.embeddingDim, params.numPSPart, params.weightedSI,
      params.numWeightsSI, params.seed, params.learningRate, params.decayRate)
  }

  def train(newEdgesSI: RDD[(Int, Int, Array[Int])], params: EGESParam, path: String): Unit = {
    //println("enter train process.")
    psWeightMatrix.checkpoint(0)
    psMatrix.checkpoint(0)
    var learningRate = params.learningRate
    for (epoch <- 1 to params.numEpoch) {
      val epochStartTime = System.currentTimeMillis()
      val (lossSum, size) = newEdgesSI.mapPartitions {
        iterator =>
          iterator.sliding(params.batchSize, params.batchSize)
            .map(batch => optimize(batch.toArray, params.negSample, params.maxIndex, learningRate))
      }.reduce((f1, f2) => (f1._1 + f2._1, f1._2 + f2._2))
      learningRate = ssScheduler.next().toFloat
      val epochTime = System.currentTimeMillis() - epochStartTime
      println(s"epoch=$epoch loss=${lossSum / size} time=${epochTime.toFloat / 1000}s")
      checkpointAndSaveIfNeed(epoch, params, path)
    }
  }

  def embeddingAggregate(newEdgesSI: RDD[(Int, Int, Array[Int])], params: EGESParam): RDD[(Int, Array[Float])] = {
    val aggregateEmbeddings = newEdgesSI.mapPartitions{
      iterator =>
        iterator.sliding(params.batchSize, params.batchSize)
          .map(batch =>
            if (weightedSI) {
              batchAggregate(batch.toArray, params.maxIndex)
            } else {
              batchAggregateEqualWeights(batch.toArray, params.maxIndex)
            }
          )
    }
    aggregateEmbeddings.flatMap(e => e)
  }

  def batchAggregate(batch: Array[(Int, Int, Array[Int])], maxIndex: Int): Array[(Int, Array[Float])] = {
    val pullBatch = EGESModel.parseBatchData(batch, 1, maxIndex)
    val (srcNodes, dstNodes, sideInfoNodes, negativeSamples) = (pullBatch._1, pullBatch._2, pullBatch._3, pullBatch._4)
    val (srcSINodesDis, dstNodesDis) = distinctNodeIds(srcNodes, dstNodes, sideInfoNodes)
    val weightsUnNorm = psWeightMatrix.asyncPsfGet(new GetEmbedding(new EmbeddingGetParam(psWeightMatrix.id,
      srcNodes.distinct))).get(1800000, TimeUnit.MILLISECONDS).asInstanceOf[EmbeddingGetResult].getResult
    val weights = weightsNormalization(weightsUnNorm)
    val result = psMatrix.asyncPsfGet(new LINEGetEmbedding(new LINEGetEmbeddingParam(matrixId, srcSINodesDis,
      dstNodesDis, negativeSamples, 2, 1))).get(1800000, TimeUnit.MILLISECONDS)
      .asInstanceOf[LINEGetEmbeddingResult].getResult
    val srcSIFeats = result._1
    val batchAggregateEmbeddings = new ArrayBuffer[(Int, Array[Float])](weights.size())
    val indices = weights.keySet().toIntArray
    val srcSIIds = new Int2ObjectOpenHashMap[Array[Int]](weights.size())
    for (i <- 0 until srcNodes.length) {
      srcSIIds.put(srcNodes(i), sideInfoNodes(i))
    }
    for (i <- 0 until weights.size()) {
      val elementAggregateEmbedding = elementAggregate(indices(i), srcSIIds.get(indices(i)), weights.get(indices(i)), srcSIFeats)
      batchAggregateEmbeddings.append(elementAggregateEmbedding)
    }
    batchAggregateEmbeddings.toArray
  }

  def batchAggregateEqualWeights(batch: Array[(Int, Int, Array[Int])], maxIndex: Int): Array[(Int, Array[Float])] = {
    val pullBatch = EGESModel.parseBatchData(batch, 1, maxIndex)
    val (srcNodes, dstNodes, sideInfoNodes, negativeSamples) = (pullBatch._1, pullBatch._2, pullBatch._3, pullBatch._4)
    val (srcSINodesDis, dstNodesDis) = distinctNodeIds(srcNodes, dstNodes, sideInfoNodes)
    val weightsEqual = Array.ofDim[Float](1+numWeightsSI).map(e => e + 1.0f/(1+numWeightsSI))
    val result = psMatrix.asyncPsfGet(new LINEGetEmbedding(new LINEGetEmbeddingParam(matrixId, srcSINodesDis,
      dstNodesDis, negativeSamples, 2, 1))).get(1800000, TimeUnit.MILLISECONDS)
      .asInstanceOf[LINEGetEmbeddingResult].getResult
    val srcSIFeats = result._1
    val indices = srcNodes.distinct
    val batchAggregateEmbeddings = new ArrayBuffer[(Int, Array[Float])](indices.length)
    val srcSIIds = new Int2ObjectOpenHashMap[Array[Int]](indices.length)
    for (i <- 0 until srcNodes.length) {
      srcSIIds.put(srcNodes(i), sideInfoNodes(i))
    }
    for (i <- 0 until indices.length) {
      val elementAggregateEmbedding = elementAggregate(indices(i), srcSIIds.get(indices(i)), weightsEqual, srcSIFeats)
      batchAggregateEmbeddings.append(elementAggregateEmbedding)
    }
    batchAggregateEmbeddings.toArray
  }

  def elementAggregate(srcId: Int, sideInfoIds: Array[Int], weights: Array[Float],
                       srcSIFeats: Int2ObjectOpenHashMap[Array[Float]]): (Int, Array[Float]) = {
    val srcSIIds = Array[Int](srcId) ++ sideInfoIds
    var aggregateEmbedding = new Array[Float](embeddingDim)
    for (i <- 0 until weights.length) {
      aggregateEmbedding = aggregateEmbedding.zip(srcSIFeats.get(srcSIIds(i))).map(e => e._1 + e._2 * weights(i))
    }
    (srcId, aggregateEmbedding)
  }

  /**
   * Write checkpoint or model result if need
   *
   * @param epoch
   */
  def checkpointAndSaveIfNeed(epoch: Int, params: EGESParam, path: String): Unit = {
    var startTs = 0L
    if (epoch % params.checkpointInterval == 0 && epoch < params.numEpoch) {
      LogUtils.logTime(s"Epoch=$epoch, checkpoint the model")
      startTs = System.currentTimeMillis()

      psWeightMatrix.checkpoint(epoch)
      psMatrix.checkpoint(epoch)
      LogUtils.logTime(s"checkpoint use time=${System.currentTimeMillis() - startTs}")
    }

    if (epoch % params.saveModelInterval == 0 && epoch < params.numEpoch) {
      LogUtils.logTime(s"Epoch=$epoch, save the model")
      startTs = System.currentTimeMillis()
      save(path, epoch)
      LogUtils.logTime(s"save use time=${System.currentTimeMillis() - startTs}")
    }
  }

  def save(modelPathRoot: String, epoch: Int): Unit = {
    save(new Path(modelPathRoot, s"CP_$epoch").toString)
  }

  def save(modelPath: String): Unit = {
    LogUtils.logTime(s"saving model to $modelPath")
    val ss = SparkSession.builder().getOrCreate()
    deleteIfExists(modelPath, ss)

    val saveContext = new ModelSaveContext(modelPath)
    saveContext.addMatrix(new MatrixSaveContext(weightMatrixName, classOf[TextEmbeddingNodeOutputFormat].getTypeName))
    saveContext.addMatrix(new MatrixSaveContext(matrixName, classOf[TextLINEModelOutputFormat].getTypeName))
    PSContext.instance().save(saveContext)
  }

  private def deleteIfExists(modelPath: String, ss: SparkSession): Unit = {
    val path = new Path(modelPath)
    val fs = path.getFileSystem(ss.sparkContext.hadoopConfiguration)
    if (fs.exists(path)) {
      fs.delete(path, true)
    }
  }

  def optimize(batch: Array[(Int, Int, Array[Int])], numNegSample: Int, maxIndex: Int, learningRate: Float):
  (Double, Long) = {
    //println("enter optimize process.")
    val trainBatch = EGESModel.parseBatchData(batch, numNegSample, maxIndex)
    val loss =
      if(weightedSI) {
        optimizeOneBatch(batch.length, trainBatch._1, trainBatch._2, trainBatch._3, trainBatch._4, numNegSample,
          learningRate)
      } else {
        optimizeOneBatchEqualWeights(batch.length, trainBatch._1, trainBatch._2, trainBatch._3, trainBatch._4,
          numNegSample, learningRate)
      }
    (loss, trainBatch._1.length.toLong)
  }

  def optimizeOneBatch(batchSize: Int, srcNodes: Array[Int], dstNodes: Array[Int], sideInfoNodes: Array[Array[Int]],
                       negativeSamples: Array[Array[Int]], numNegSample: Int, learningRate: Float): Double = {
    incCallNum()

    var start = 0L
    start = System.currentTimeMillis()
    val weights = psWeightMatrix.asyncPsfGet(new GetEmbedding(new EmbeddingGetParam(psWeightMatrix.id,
      srcNodes.distinct))).get(1800000, TimeUnit.MILLISECONDS).asInstanceOf[EmbeddingGetResult].getResult
    incWeightsPullTime(start)
    start = System.currentTimeMillis()
    val expWeights = weightsNormalization(weights)
    incWeightsNormTime(start)
    println("the elements of expWeights are")
    expWeights.get(srcNodes(0)).foreach(e => print(e + ", "))
    print("\n")

    val (srcSINodesDis, dstNodesDis) = distinctNodeIds(srcNodes, dstNodes, sideInfoNodes)
    start = System.currentTimeMillis()
    val result = psMatrix.asyncPsfGet(new LINEGetEmbedding(new LINEGetEmbeddingParam(matrixId, srcSINodesDis,
      dstNodesDis, negativeSamples, 2, numNegSample))).get(1800000, TimeUnit.MILLISECONDS)
      .asInstanceOf[LINEGetEmbeddingResult].getResult
    val srcFeats: Int2ObjectOpenHashMap[Array[Float]] = result._1
    val dstFeats: Int2ObjectOpenHashMap[Array[Float]] = result._2
    incFeatsPullTime(start)

    // Calculate the gradients
    start = System.currentTimeMillis()
    // weighted sum of feats of srcNode and SINodes
    val hFeats = createHFeats(srcNodes, sideInfoNodes, srcFeats, expWeights)
    val dots = dot(srcNodes, dstNodes, negativeSamples, hFeats, dstFeats, numNegSample)
    var loss = doGrad(dots, numNegSample, learningRate)
    incCalTime(start)
    start = System.currentTimeMillis()
    val (inputUpdates, outputUpdates, weightUpdates) = adjust(srcNodes, dstNodes, sideInfoNodes, negativeSamples,
      srcFeats, dstFeats, numNegSample, dots, expWeights, hFeats)
    incCalUpdateTime(start)

    // Push the gradient to ps
    start = System.currentTimeMillis()
    psMatrix.psfUpdate(new LINEAdjust(new LINEAdjustParam(matrixId, inputUpdates, outputUpdates, 2)))
    incFeatsPushTime(start)
    start = System.currentTimeMillis()
    //val weightDelta = VFactory.denseFloatVector(weightUpdates)
    psWeightMatrix.psfUpdate(new AdjustEmbedding(new EmbeddingAdjustParam(psWeightMatrix.id, weightUpdates,
      numWeightsSI+1, 1.0f)))
    incWeightsPushTime(start)

    loss = loss / dots.length
    println(s"avgWeightsPullTime=$avgWeightsPullTime avgWeightsNormTime=$avgWeightsNormTime " +
      s"avgFeatsPullTime=$avgFeatsPullTime avgGradTime=$avgCalTime avgCalUpdateTime=$avgCalUpdateTime " +
      s"avgWeightsPushTime=$avgWeightsPushTime avgFeatsPushTime=$avgFeatsPushTime loss=$loss")

    loss * batchSize
  }

  def optimizeOneBatchEqualWeights(batchSize: Int, srcNodes: Array[Int], dstNodes: Array[Int],
                                   sideInfoNodes: Array[Array[Int]], negativeSamples: Array[Array[Int]],
                                   numNegSample: Int, learningRate: Float): Double = {
    //println("enter optimizeOneBatch process.")
    incCallNum()
    var start = 0L
    val weights = Array.ofDim[Float](1 + numWeightsSI)
    val expWeights = weights.map(e => e + 1.0f/(1 + numWeightsSI))
    println("the elements of expWeights are:")
    expWeights.foreach(e => print(e + " "))
    print("\n")
    // remove duplicates and avoid duplicated pulling
    val (srcSINodesDis, dstNodesDis) = distinctNodeIds(srcNodes, dstNodes, sideInfoNodes)
    start = System.currentTimeMillis()

    val result = psMatrix.asyncPsfGet(new LINEGetEmbedding(new LINEGetEmbeddingParam(matrixId, srcSINodesDis,
      dstNodesDis, negativeSamples, 2, numNegSample))).get(1800000, TimeUnit.MILLISECONDS)
      .asInstanceOf[LINEGetEmbeddingResult].getResult
    val srcFeats: Int2ObjectOpenHashMap[Array[Float]] = result._1
    val dstFeats: Int2ObjectOpenHashMap[Array[Float]] = result._2
    incFeatsPullTime(start)

    // Calculate the gradients
    start = System.currentTimeMillis()
    // weighted sum of feats of srcNode and SINodes
    val hFeats = createHFeats(srcNodes, sideInfoNodes, srcFeats, expWeights)
    val dots = dot(srcNodes, dstNodes, negativeSamples, hFeats, dstFeats, numNegSample)
    var loss = doGrad(dots, numNegSample, learningRate)
    incCalTime(start)
    start = System.currentTimeMillis()
    val (inputUpdates, outputUpdates) = adjustWithoutWeights(srcNodes, dstNodes, sideInfoNodes, negativeSamples,
      srcFeats, dstFeats, numNegSample, dots, expWeights, hFeats)
    incCalUpdateTime(start)
    // Push the gradient to ps
    start = System.currentTimeMillis()
    psMatrix.psfUpdate(new LINEAdjust(new LINEAdjustParam(matrixId, inputUpdates, outputUpdates, 2)))
    incFeatsPushTime(start)

    loss = loss / dots.length
    println(s"avgFeatsPullTime=$avgFeatsPullTime avgGradTime=$avgCalTime " +
      s"avgCalUpdateTime=$avgCalUpdateTime avgFeatsPushTime=$avgFeatsPushTime loss=$loss")

    loss * batchSize
  }

  def distinctNodeIds(srcNodeIds: Array[Int], dstNodeIds: Array[Int], SINodeIds: Array[Array[Int]]):
  (Array[Int], Array[Int]) = {
    val dstArray = dstNodeIds.distinct
    val srcSIArray = (srcNodeIds ++ SINodeIds.flatMap(row => row)).distinct
    (srcSIArray, dstArray)
  }

  def createHFeats(srcNodes: Array[Int], sideInfoNodes: Array[Array[Int]],
                   srcFeats: Int2ObjectOpenHashMap[Array[Float]],
                   expWeights: Int2ObjectOpenHashMap[Array[Float]]): Int2ObjectOpenHashMap[Array[Float]] = {
    val hFeats = new Int2ObjectOpenHashMap[Array[Float]](srcNodes.length)
    for (i <- srcNodes.indices) {
      val weights = expWeights.get(srcNodes(i))
      var feats = srcFeats.get(srcNodes(i)).clone().map(t => t * weights(0))
      for (s <- sideInfoNodes(i).indices) {
        feats = feats.zip(srcFeats.get(sideInfoNodes(i)(s))).map(t => t._1 + t._2 * weights(s+1))
      }
      hFeats.put(srcNodes(i),feats)
    }
    hFeats
  }

  def createHFeats(srcNodes: Array[Int], sideInfoNodes: Array[Array[Int]],
                   srcFeats: Int2ObjectOpenHashMap[Array[Float]],
                   expWeights: Array[Float]): Int2ObjectOpenHashMap[Array[Float]] = {
    val hFeats = new Int2ObjectOpenHashMap[Array[Float]](srcNodes.length)
    for (i <- srcNodes.indices) {
      var feats = srcFeats.get(srcNodes(i)).clone().map(t => t * expWeights(0))
      for (s <- sideInfoNodes(i).indices) {
        feats = feats.zip(srcFeats.get(sideInfoNodes(i)(s))).map(t => t._1 + t._2 * expWeights(s+1))
      }
      hFeats.put(srcNodes(i),feats)
    }
    hFeats
  }

  def dot(srcNodes: Array[Int], dstNodes: Array[Int], negativeSamples: Array[Array[Int]],
          srcFeats: Int2ObjectOpenHashMap[Array[Float]], targetFeats: Int2ObjectOpenHashMap[Array[Float]],
          negative: Int): Array[Float] = {
    val dots = new Array[Float]((1 + negative) * srcNodes.length)
    var docIndex = 0
    for (i <- srcNodes.indices) {
      val srcVec = srcFeats.get(srcNodes(i))
      // Get dot value for (src, dst)
      dots(docIndex) = arraysDot(srcVec, targetFeats.get(dstNodes(i)))
      docIndex += 1

      // Get dot value for (src, negative sample)
      for (j <- 0 until negative) {
        dots(docIndex) = arraysDot(srcVec, targetFeats.get(negativeSamples(i)(j)))
        docIndex += 1
      }
    }
    dots
  }

  def arraysDot(x: Array[Float], y: Array[Float]): Float = {
    var dotValue = 0.0f
    x.indices.foreach(i => dotValue += x(i) * y(i))
    dotValue
  }

  def doGrad(dots: Array[Float], negative: Int, alpha: Float): Float = {
    var loss = 0.0f
    for (i <- dots.indices) {
      val prob = FastSigmoid.sigmoid(dots(i))// prob = sigma(H,Z)
      if (i % (negative + 1) == 0) {// dstNode
        dots(i) = alpha * (1 - prob) // negative gradient with stepsize
        loss -= FastSigmoid.log(prob)
      } else {// negativeSample Node
        dots(i) = -alpha * prob
        loss -= FastSigmoid.log(1 - prob)
      }
    }
    loss
  }

  def adjust(srcNodes: Array[Int], destNodes: Array[Int], sideInfoNodes: Array[Array[Int]],
             negativeSamples: Array[Array[Int]], srcFeats: Int2ObjectOpenHashMap[Array[Float]],
             targetFeats: Int2ObjectOpenHashMap[Array[Float]], negative: Int, dots: Array[Float],
             expWeights: Int2ObjectOpenHashMap[Array[Float]], hFeats: Int2ObjectOpenHashMap[Array[Float]]):
  (Int2ObjectOpenHashMap[Array[Float]], Int2ObjectOpenHashMap[Array[Float]], Int2ObjectOpenHashMap[Array[Float]]) = {
    val inputUpdateCounter = new Int2IntOpenHashMap(srcFeats.size())
    val inputUpdates = new Int2ObjectOpenHashMap[Array[Float]](srcFeats.size())

    val outputUpdateCounter = new Int2IntOpenHashMap(targetFeats.size())
    val outputUpdates = new Int2ObjectOpenHashMap[Array[Float]](targetFeats.size())

    val inputWeightCounter = new Int2IntOpenHashMap(expWeights.size())
    val inputWeightUpdates = new Int2ObjectOpenHashMap[Array[Float]](expWeights.size())

    var docIndex = 0
    for (i <- srcNodes.indices) {
      // Src node grad
      val neule = new Array[Float](embeddingDim)

      // Accumulate dst node embedding to neule
      val dstEmbedding = targetFeats.get(destNodes(i))
      var g = dots(docIndex)
      axpy(neule, dstEmbedding, g)

      // Use src node embedding to update dst node embedding
      val srcEmbedding = srcFeats.get(srcNodes(i))
      merge(outputUpdateCounter, outputUpdates, destNodes(i), g, srcEmbedding)

      docIndex += 1

      for (j <- 0 until negative) {
        val negSampleEmbedding = targetFeats.get(negativeSamples(i)(j))
        g = dots(docIndex)

        // accumulate embeddings of negative samples to neule
        axpy(neule, negSampleEmbedding, g)

        // update embeddings of negative samples
        merge(outputUpdateCounter, outputUpdates, negativeSamples(i)(j), g, srcEmbedding)
        docIndex += 1
      }

      // update src node embedding
      mergeSrcSI(inputUpdateCounter, inputUpdates, srcNodes(i), sideInfoNodes(i), expWeights.get(srcNodes(i)), 1, neule)
      mergeWeight(inputWeightCounter, inputWeightUpdates, srcNodes(i), sideInfoNodes(i), expWeights.get(srcNodes(i)),
        srcFeats, hFeats, 1, neule)
    }

    var iter = inputUpdateCounter.int2IntEntrySet().fastIterator()
    while (iter.hasNext) {
      val entry = iter.next()
      div(inputUpdates.get(entry.getIntKey), entry.getIntValue.toFloat)
    }

    iter = outputUpdateCounter.int2IntEntrySet().fastIterator()
    while (iter.hasNext) {
      val entry = iter.next()
      div(outputUpdates.get(entry.getIntKey), entry.getIntValue.toFloat)
    }

    iter = inputWeightCounter.int2IntEntrySet().fastIterator()
    while (iter.hasNext) {
      val entry = iter.next()
      div(inputWeightUpdates.get(entry.getIntKey), entry.getIntValue.toFloat)
    }
    (inputUpdates, outputUpdates, inputWeightUpdates)
  }

  def adjustWithoutWeights(srcNodes: Array[Int], destNodes: Array[Int], sideInfoNodes: Array[Array[Int]],
                           negativeSamples: Array[Array[Int]], srcFeats: Int2ObjectOpenHashMap[Array[Float]],
                           targetFeats: Int2ObjectOpenHashMap[Array[Float]], negative: Int, dots: Array[Float],
                           expWeights: Array[Float], hFeats: Int2ObjectOpenHashMap[Array[Float]]):
  (Int2ObjectOpenHashMap[Array[Float]], Int2ObjectOpenHashMap[Array[Float]]) = {
    val inputUpdateCounter = new Int2IntOpenHashMap(srcFeats.size())
    val inputUpdates = new Int2ObjectOpenHashMap[Array[Float]](srcFeats.size())

    val outputUpdateCounter = new Int2IntOpenHashMap(targetFeats.size())
    val outputUpdates = new Int2ObjectOpenHashMap[Array[Float]](targetFeats.size())

    val inputWeightUpdates = new Int2ObjectOpenHashMap[Array[Float]](srcFeats.size())

    var docIndex = 0
    for (i <- srcNodes.indices) {
      // Src node grad
      val neule = new Array[Float](embeddingDim)

      // Accumulate dst node embedding to neule
      val dstEmbedding = targetFeats.get(destNodes(i))
      var g = dots(docIndex)
      axpy(neule, dstEmbedding, g)

      // Use src node embedding to update dst node embedding
      val srcEmbedding = srcFeats.get(srcNodes(i))
      merge(outputUpdateCounter, outputUpdates, destNodes(i), g, srcEmbedding)

      docIndex += 1

      for (j <- 0 until negative) {
        val negSampleEmbedding = targetFeats.get(negativeSamples(i)(j))
        g = dots(docIndex)

        // accumulate embeddings of negative samples
        axpy(neule, negSampleEmbedding, g)

        // update embeddings of negative samples
        merge(outputUpdateCounter, outputUpdates, negativeSamples(i)(j), g, srcEmbedding)
        docIndex += 1
      }

      // update embeddings of src nodes
      mergeSrcSI(inputUpdateCounter, inputUpdates, srcNodes(i), sideInfoNodes(i), expWeights, 1, neule)
      mergeWeight(inputWeightUpdates, srcNodes(i), sideInfoNodes(i), expWeights, srcFeats, hFeats, 1, neule)
    }

    var iter = inputUpdateCounter.int2IntEntrySet().fastIterator()
    while (iter.hasNext) {
      val entry = iter.next()
      div(inputUpdates.get(entry.getIntKey), entry.getIntValue.toFloat)
    }

    iter = outputUpdateCounter.int2IntEntrySet().fastIterator()
    while (iter.hasNext) {
      val entry = iter.next()
      div(outputUpdates.get(entry.getIntKey), entry.getIntValue.toFloat)
    }

    (inputUpdates, outputUpdates)
  }

  def merge(inputUpdateCounter: Int2IntOpenHashMap, inputUpdates: Int2ObjectOpenHashMap[Array[Float]],
            nodeId: Int, g: Float, update: Array[Float]): Unit = {
    var grads: Array[Float] = inputUpdates.get(nodeId)
    if (grads == null) {
      grads = new Array[Float](embeddingDim)
      inputUpdates.put(nodeId, grads)
      inputUpdateCounter.put(nodeId, 0)
    }
    axpy(grads, update, g)
    inputUpdateCounter.addTo(nodeId, 1)
  }

  def mergeSrcSI(inputUpdateCounter: Int2IntOpenHashMap, inputUpdates: Int2ObjectOpenHashMap[Array[Float]],
                 nodeId: Int, SINodeIds: Array[Int], expWeights: Array[Float], g: Float, update: Array[Float]): Unit = {
    val nodeIds = Array(nodeId) ++ SINodeIds
    for (i <- nodeIds.indices) {
      val nodeId = nodeIds(i)
      var grads: Array[Float] = inputUpdates.get(nodeId)
      if (grads == null) {
        grads = new Array[Float](embeddingDim)
        inputUpdates.put(nodeId, grads)
        inputUpdateCounter.put(nodeId, 0)
      }
      val weightedUpdate = update.map(t => t * expWeights(i))
      axpy(grads, weightedUpdate, g)
      inputUpdateCounter.addTo(nodeId, 1)
    }
  }

  def mergeWeight(inputWeightCounter: Int2IntOpenHashMap, inputWeightUpdates: Int2ObjectOpenHashMap[Array[Float]],
                  nodeId: Int, SINodeIds: Array[Int], expWeights: Array[Float],
                  srcFeats: Int2ObjectOpenHashMap[Array[Float]], hFeats: Int2ObjectOpenHashMap[Array[Float]],
                  g: Float, update: Array[Float]): Unit = {
    val nodeIds = Array(nodeId) ++ SINodeIds
    var grads: Array[Float] = inputWeightUpdates.get(nodeId)
    val weightGrads = new Array[Float](nodeIds.length)
    if (grads == null) {
      grads = new Array[Float](nodeIds.length)
      inputWeightUpdates.put(nodeId, grads)
      inputWeightCounter.put(nodeId, 0)
    }
    for (i <- nodeIds.indices) {
      val grad = srcFeats.get(nodeIds(i)).clone().map(t => t*expWeights(i)).zip(hFeats.get(nodeId).clone().map(t =>
        t*expWeights(i))).map(t => t._1-t._2) // partial(H,weight)
      weightGrads(i) = update.zip(grad).map(t => t._1 * t._2).sum
    }
    axpy(grads, weightGrads, g)
    inputWeightCounter.addTo(nodeId, 1)
  }

  def mergeWeight(inputWeightUpdates: Int2ObjectOpenHashMap[Array[Float]], nodeId: Int, SINodeIds: Array[Int],
                  expWeights: Array[Float], srcFeats: Int2ObjectOpenHashMap[Array[Float]],
                  hFeats: Int2ObjectOpenHashMap[Array[Float]], g: Float, update: Array[Float]): Unit = {
    val nodeIds = Array(nodeId) ++ SINodeIds
    var grads: Array[Float] = inputWeightUpdates.get(nodeId)
    val weightGrads = new Array[Float](nodeIds.length)
    if (grads == null) {
      grads = new Array[Float](nodeIds.length)
      inputWeightUpdates.put(nodeId, grads)
    }
    for (i <- nodeIds.indices) {
      val grad = srcFeats.get(nodeIds(i)).clone().map(t => t*expWeights(i)).zip(hFeats.get(nodeId).clone().map(t =>
        t*expWeights(i))).map(t => t._1-t._2) // partial(H,weight)
      weightGrads(i) = update.zip(grad).map(t => t._1 * t._2).sum
    }
    axpy(grads, weightGrads, g)
  }

  def axpy(y: Array[Float], x: Array[Float], a: Float): Unit = {
    x.indices.foreach(i => y(i) += a * x(i))
  }

  def div(x: Array[Float], f: Float): Unit = {
    x.indices.foreach(i => x(i) = x(i) / f)
  }

  def weightsNormalization(weights: Int2ObjectOpenHashMap[Array[Float]]): Int2ObjectOpenHashMap[Array[Float]] = {
    val keys = weights.keySet().toIntArray()
    var weightsElement = new Array[Float](0)
    for(key <- keys) {
      weightsElement = weights.get(key)
      val expSum = weightsElement.map(e => math.exp(e)).sum
      val normWeightsElement = weightsElement.map(e => (math.exp(e) / expSum).toFloat)
      weights.put(key, normWeightsElement)
    }
    weights
  }

  def load(modelPath: String): Unit = {
    val startTime = System.currentTimeMillis()
    LogUtils.logTime(s"load model from $modelPath")

    val loadContext = new ModelLoadContext(modelPath)
    loadContext.addMatrix(new MatrixLoadContext(psMatrix.name))
    PSContext.getOrCreate(SparkContext.getOrCreate()).load(loadContext)
    LogUtils.logTime(s"model load time=${System.currentTimeMillis() - startTime} ms")
  }

  /* time calculate functions */
  def incWeightsPullTime(startTs: Long): Unit = {
    totalWeightsPullTime = totalWeightsPullTime + (System.currentTimeMillis() - startTs)
  }

  def incWeightsNormTime(startTs: Long): Unit = {
    totalWeightsNormTime = totalWeightsNormTime + (System.currentTimeMillis() - startTs)
  }

  def incFeatsPullTime(startTs: Long): Unit = {
    totalFeatsPullTime = totalFeatsPullTime + (System.currentTimeMillis() - startTs)
  }

  def incWeightsPushTime(startTs: Long): Unit = {
    totalWeightsPushTime = totalWeightsPushTime + (System.currentTimeMillis() - startTs)
  }

  def incFeatsPushTime(startTs: Long): Unit = {
    totalFeatsPushTime = totalFeatsPushTime + (System.currentTimeMillis() - startTs)
  }

  def incCalTime(startTs: Long): Unit = {
    totalCalTime = totalCalTime + (System.currentTimeMillis() - startTs)
  }

  def incCalUpdateTime(startTs: Long): Unit = {
    totalMakeGradTime = totalMakeGradTime + (System.currentTimeMillis() - startTs)
  }

  def incCallNum(): Unit = {
    totalCallNum = totalCallNum + 1
  }

  def avgWeightsPullTime: Long = {
    totalWeightsPullTime / totalCallNum
  }

  def avgWeightsNormTime: Long = {
    totalWeightsNormTime / totalCallNum
  }

  def avgFeatsPullTime: Long = {
    totalFeatsPullTime / totalCallNum
  }

  def avgWeightsPushTime: Long = {
    totalWeightsPushTime / totalCallNum
  }

  def avgFeatsPushTime: Long = {
    totalFeatsPushTime / totalCallNum
  }

  def avgCalUpdateTime: Long = {
    totalMakeGradTime / totalCallNum
  }

  def avgCalTime: Long = {
    totalCalTime / totalCallNum
  }

  def incMakeParamTime(startTs: Long): Unit = {
    totalMakeParamTime = totalMakeParamTime + (System.currentTimeMillis() - startTs)
  }

  def avgMakeParamTime: Long = {
    totalMakeParamTime / totalCallNum
  }
}
object EGESModel {

  def parseBatchData(edgeSIs: Array[(Int, Int, Array[Int])], negative: Int, maxIndex: Int, seed: Int = Random.nextInt):
  (Array[Int], Array[Int], Array[Array[Int]], Array[Array[Int]]) = {
    val rand = new Random(seed)
    val srcNodes = new ArrayBuffer[Int]()
    val dstNodes = new ArrayBuffer[Int]()
    val SIs = new ArrayBuffer[Array[Int]]()
    for (row <- edgeSIs) {
      srcNodes.append(row._1)
      dstNodes.append(row._2)
      SIs.append(row._3)
    }
    val negativeSamples = negativeSample(srcNodes.toArray, dstNodes.toArray, negative, maxIndex, rand.nextInt())
    (srcNodes.toArray, dstNodes.toArray, SIs.toArray, negativeSamples)
  }

  def negativeSample(srcNodes: Array[Int], dstNodes: Array[Int], sampleNum: Int, maxIndex: Int, seed: Int):
  Array[Array[Int]] = {
    val rand = new Random(seed)
    val sampleWords = new Array[Array[Int]](srcNodes.length)
    var wordIndex: Int = 0

    for (i <- srcNodes.indices) {
      var sampleIndex: Int = 0
      sampleWords(wordIndex) = new Array[Int](sampleNum)
      while (sampleIndex < sampleNum) {
        val target = rand.nextInt(maxIndex)
        if (target != srcNodes(i) && target != dstNodes(i)) {
          sampleWords(wordIndex)(sampleIndex) = target
          sampleIndex += 1
        }
      }
      wordIndex += 1
    }
    sampleWords
  }

}
