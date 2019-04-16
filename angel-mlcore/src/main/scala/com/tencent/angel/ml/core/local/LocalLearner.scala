package com.tencent.angel.ml.core.local

import com.tencent.angel.ml.core.conf.{MLCoreConf, SharedConf}
import com.tencent.angel.ml.core.{GraphModel, Learner}
import com.tencent.angel.ml.core.data.{DataBlock, DataReader}
import com.tencent.angel.ml.core.network.Graph
import com.tencent.angel.ml.core.variable.VoidType
import com.tencent.angel.ml.core.optimizer.decayer.{StepSizeScheduler, WarmRestarts}
import com.tencent.angel.ml.core.utils.ValidationUtils
import com.tencent.angel.ml.math2.utils.LabeledData
import org.apache.commons.logging.{Log, LogFactory}
import org.apache.hadoop.conf.Configuration

class LocalLearner(conf: SharedConf) extends Learner {
  private val LOG: Log = LogFactory.getLog(classOf[LocalLearner])

  // 1. initial model, model can be view as a proxy of graph
  val model: LocalModel = new LocalModel(conf)
  val graph: Graph = model.graph

  // 2. build network
  model.buildNetwork()

  // 3. init or load matrices
  private val modelPath: String = conf.get(MLCoreConf.ML_LOAD_MODEL_PATH, MLCoreConf.DEFAULT_ML_LOAD_MODEL_PATH)
  private val actionType: String = conf.get(MLCoreConf.ML_ACTION_TYPE, MLCoreConf.DEFAULT_ML_ACTION_TYPE)
  private val env = new LocalEnvContext
  if (actionType.equalsIgnoreCase("train") && modelPath.isEmpty) {
    model.createMatrices(env)
    model.init(env)
  } else {
    model.loadModel(env, modelPath, new Configuration())
  }

  private val lr0 = SharedConf.learningRate
  override protected val ssScheduler: StepSizeScheduler = new WarmRestarts(lr0, lr0/100, 0.001)

  override protected def trainOneEpoch(epoch: Int, iter: Iterator[Array[LabeledData]], numBatch: Int): Double = {
    var batchCount: Int = 0
    var loss: Double = 0.0

    while (iter.hasNext) {
      // LOG.info("start to feedData ...")
      graph.feedData(iter.next())

      // LOG.info("start to pullParams ...")
      if (model.isSparseFormat) {
        model.pullParams(epoch, graph.placeHolder.getIndices)
      } else {
        model.pullParams(epoch)
      }

      // LOG.info("calculate to forward ...")
      loss = graph.calForward() // forward
      println(s"The training los of epoch $epoch batch $batchCount is $loss")
      LOG.info(s"The training los of epoch $epoch batch $batchCount is $loss")

      // LOG.info("calculate to backward ...")
      graph.calBackward() // backward

      // LOG.info("calculate and push gradient ...")
      model.pushGradient(graph.getLR) // pushgrad
      // waiting all gradient pushed

      // LOG.info("waiting for push barrier ...")
      // barrier(0, graph)
      graph.setLR(ssScheduler.next())
      // LOG.info("start to update ...")
      model.update[VoidType](epoch * numBatch + batchCount, graph.placeHolder.getBatchSize) // update parameters on PS

      // waiting all gradient update finished
      // LOG.info("waiting for update barrier ...")
      // barrier(0, graph)
      batchCount += 1

      // LOG.info(s"epoch $epoch batch $batchCount is finished!")
    }

    loss
  }

  override def train(posTrainData: DataBlock[LabeledData], negTrainData: DataBlock[LabeledData], validationData: DataBlock[LabeledData]): GraphModel = {
    val numBatch: Int = SharedConf.numUpdatePerEpoch
    val batchSize: Int = if (negTrainData == null) {
      (posTrainData.size() + numBatch - 1) / numBatch
    } else {
      (posTrainData.size() + negTrainData.size() + numBatch - 1) / numBatch
    }
    val numEpoch: Int = SharedConf.epochNum
    val batchData: Array[LabeledData] = new Array[LabeledData](batchSize)

    var loss: Double = 0.0
    (0 until numEpoch).foreach { epoch =>
      val iter: Iterator[Array[LabeledData]] = if (negTrainData == null) {
        DataReader.getBathDataIterator(posTrainData, batchData, numBatch)
      } else {
        DataReader.getBathDataIterator(posTrainData, negTrainData, batchData, numBatch)
      }

      preHook.foreach(func => func(graph))
      loss += trainOneEpoch(epoch, iter, numBatch)
      postHook.foreach(func => func(graph))

      validate(epoch, validationData)
    }

    model
  }

  override protected def validate(epoch: Int, valiData: DataBlock[LabeledData]): Unit = {
    ValidationUtils.calMetrics(epoch, model.predict(valiData), graph.getLossFunc)
  }

  override protected def barrier(): Unit = ???
}
