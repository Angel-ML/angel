package com.tencent.angel.graph.embedding.struct2vec

class Struct2vecParams extends Serializable {

  var partitionNum: Int = _
  var walkLength: Int = _
  var stay_prob: Float = _
  var opt1_reduce_len: Boolean = true
  var opt2_reduce_sim_calc: Boolean = false
  var opt3_num_layers:Int = _
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
  var extraContextEmbeddingPath: String = _
  var checkpointInterval: Int = Int.MaxValue
  var saveModelInterval: Int = Int.MaxValue
  var order: Int = _
  var nodesNumPerRow: Int = -1
  var numRowDataSet: Option[Long] = None
  var seed: Int = _
  var maxLength: Int = -1
  var nodeTypePath: String = ""
  var saveContextEmbedding: Boolean = _
  var output:String = _

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

  def setWalkLength(walkLength: Int): this.type = {
    require(walkLength > 0, s"require walkLength > 0, $walkLength given")
    this.walkLength = walkLength
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
    require(maxIndex > 0 && maxIndex < Int.MaxValue, s"require maxIndex > 0 && maxIndex < Int.maxValue, $maxIndex given")
    this.maxIndex = maxIndex.toInt
    this
  }

  def setMinIndex(minIndex: Long): this.type = {
    require(minIndex >= 0 && minIndex < Int.MaxValue, s"require minIndex >= 0 && minIndex < Int.maxValue, $minIndex given")
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

  def setExtraContextEmbeddingPath(extraContextEmbeddingPath: String): this.type = {
    this.extraContextEmbeddingPath = extraContextEmbeddingPath
    this
  }

  def setNodeTypePath(nodeTypePath: String): this.type = {
    this.nodeTypePath = nodeTypePath
    this
  }

  def setSaveContextEmbedding(saveContextEmbedding: Boolean): this.type = {
    this.saveContextEmbedding = saveContextEmbedding
    this
  }

  def setopt1_reduce_len(opt1_reduce_len: Boolean): this.type = {
    this.opt1_reduce_len = opt1_reduce_len
    this
  }

  def setopt2_reduce_sim_calc(opt2_reduce_sim_calc: Boolean): this.type = {
    this.opt2_reduce_sim_calc = opt2_reduce_sim_calc
    this
  }

  def setopt3_num_layers(opt3_num_layers: Int): this.type = {
    this.opt3_num_layers = opt3_num_layers
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

  def setoutPut(output: String): this.type = {
    this.output = output
    this
  }


}
