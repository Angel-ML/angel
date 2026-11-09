package com.tencent.angel.spark.ml.graph.algo.embedding.word2vec

import org.apache.spark.rdd.RDD

class Word2VecParam extends Serializable {

  var partitionNum: Int = _
  var windowSize: Int = _
  var embeddingDim: Int = _
  var negSample: Int = _
  var learningRate: Float = _
  var decayRate: Float = _
  var batchSize: Int = _
  var logStep: Int = _
  var numEpoch: Int = _
  var maxIndex: Int = _
  var minIndex: Int = _
  var sampleRate: Float = _
  var numPSPart: Int = 1
  var modelPath: String = _
  var extraInputEmbeddingPath: String = _
  var extraOutputEmbeddingPath: String = _
  var checkpointInterval: Int = Int.MaxValue
  var saveModelInterval: Int = Int.MaxValue
  var order: Int = _
  var nodesNumPerRow: Int = -1
  var numRowDataSet: Option[Long] = None
  var seed: Int = _
  var maxLength: Int = -1
  var nodeTypePath: String = ""
  var saveOutputEmbedding: Boolean = _
  var gradAvg: Boolean = _
  var partitionOpt: Boolean = _
  var localSample: Boolean = _
  var kernel: Int = 1
  var postMapping: Boolean = false
  var denseToString: Option[RDD[(Int, String)]] = None
  var hasIndexColumn: Boolean = false
  var outputEmbeddingByNodeType: Boolean = false
  var nodeTypeRDD: Option[RDD[(Int, Int)]] = None

  def setNodeTypeRDD(nodeTypeRDD: Option[RDD[(Int, Int)]]): this.type = {
    this.nodeTypeRDD = nodeTypeRDD
    this
  }

  def setOutputEmbeddingByNodeType(flag: Boolean): this.type = {
    this.outputEmbeddingByNodeType = flag
    this
  }

  def setHasIndexColumn(flag: Boolean): this.type = {
    this.hasIndexColumn = flag
    this
  }

  def setDenseToString(denseToString: Option[RDD[(Int, String)]]): this.type = {
    this.denseToString = denseToString
    this
  }

  def setPostMapping(postMapping: Boolean): this.type = {
    this.postMapping = postMapping
    this
  }

  def setMaxLength(maxLength: Int): this.type = {
    this.maxLength = maxLength
    this
  }

  def setNumRowDataSet(numRowDataSet: Long): this.type = {
    this.numRowDataSet = Some(numRowDataSet)
    this
  }

  def setSeed(seed: Int): this.type = {
    this.seed = seed
    this
  }

  def setPartitionNum(partitionNum: Int): this.type = {
    require(partitionNum > 0, s"require partitionNum > 0, $partitionNum given")
    this.partitionNum = partitionNum
    this
  }

  def setWindowSize(windowSize: Int): this.type = {
    require(windowSize > 0, s"require windowSize > 0, $windowSize given")
    this.windowSize = windowSize
    this
  }

  def setEmbeddingDim(embeddingDim: Int): this.type = {
    require(embeddingDim > 0, s"require embedding dimension > 0, $embeddingDim given")
    this.embeddingDim = embeddingDim
    this
  }

  def setNegSample(negSample: Int): this.type = {
    require(negSample > 0, s"require num of negative sample > 0, $negSample given")
    this.negSample = negSample
    this
  }

  def setLearningRate(learningRate: Float): this.type = {
    require(learningRate > 0, s"require learning rate > 0, $learningRate given")
    this.learningRate = learningRate
    this
  }

  def setDecayRate(decayRate: Float): this.type = {
    require(decayRate > 0, s"require decay rate > 0, $decayRate given")
    this.decayRate = decayRate
    this
  }

  def setBatchSize(batchSize: Int): this.type = {
    require(batchSize > 0, s"require batch size > 0, $batchSize given")
    this.batchSize = batchSize
    this
  }

  def setLogStep(logStep: Int): this.type = {
    require(logStep > 0, s"require log step > 0, $logStep given")
    this.logStep = logStep
    this
  }

  def setNumEpoch(numEpoch: Int): this.type = {
    require(numEpoch > 0, s"require num of epoch > 0, $numEpoch given")
    this.numEpoch = numEpoch
    this
  }

  def setMaxIndex(maxIndex: Long): this.type = {
    require(maxIndex > Int.MinValue && maxIndex < Int.MaxValue, s"require maxIndex > Int.MinValue && maxIndex < Int.maxValue, $maxIndex given")
    this.maxIndex = maxIndex.toInt
    this
  }

  def setMinIndex(minIndex: Long): this.type = {
    require(minIndex > Int.MinValue && minIndex < Int.MaxValue, s"require minIndex > Int.MinValue && minIndex < Int.maxValue, $minIndex given")
    this.minIndex = minIndex.toInt
    this
  }

  def setSampleRate(sampleRate: Float): this.type = {
    require(sampleRate > 0, s"sample rate belongs to [0, 1], $sampleRate given")
    this.sampleRate = sampleRate
    this
  }

  def setModelPath(modelPath: String): this.type = {
    require(null != modelPath && modelPath.nonEmpty, s"require non empty path to save model, $modelPath given")
    this.modelPath = modelPath
    this
  }

  def setExtraInputEmbeddingPath(extraInputEmbeddingPath: String): this.type = {
    this.extraInputEmbeddingPath = extraInputEmbeddingPath
    this
  }

  def setExtraOutputEmbeddingPath(extraOutputEmbeddingPath: String): this.type = {
    this.extraOutputEmbeddingPath = extraOutputEmbeddingPath
    this
  }

  def setGradAvg(gradAvg: Boolean): this.type = {
    this.gradAvg = gradAvg
    this
  }

  def setPartitionOpt(partitionOpt: Boolean): this.type = {
    this.partitionOpt = partitionOpt
    this
  }

  def setLocalSample(sample: Boolean): this.type = {
    this.localSample = sample
    this
  }

  def setKernel(kernel: Int): this.type = {
    this.kernel = kernel
    this
  }

  def setNodeTypePath(nodeTypePath: String): this.type = {
    this.nodeTypePath = nodeTypePath
    this
  }

  def setSaveOutputEmbedding(saveOutputEmbedding: Boolean): this.type = {
    this.saveOutputEmbedding = saveOutputEmbedding
    this
  }

  def setModelCPInterval(modelCPInterval: Int): this.type = {
    require(modelCPInterval > 0, s"model checkpoint interval > 0, $modelCPInterval given")
    this.checkpointInterval = modelCPInterval
    this
  }

  def setModelSaveInterval(modelSaveInterval: Int): this.type = {
    require(modelSaveInterval > 0, s"model save interval > 0, $modelSaveInterval given")
    this.saveModelInterval = modelSaveInterval
    this
  }

  def setOrder(order: Int): this.type = {
    require(order == 1 || order == 2, s"order equals 1 or 2, $order given")
    this.order = order
    this
  }

  def setNumPSPart(numPSPart: Option[Int]): this.type = {
    require(numPSPart.fold(true)(_ > 0), s"require num of PS part > 0, $numPSPart given")
    numPSPart.foreach(this.numPSPart = _)
    this
  }

  def setNodesNumPerRow(nodesNumPerRow: Option[Int]): this.type = {
    nodesNumPerRow.foreach(this.nodesNumPerRow = _)
    this
  }


}
