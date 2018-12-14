package com.tencent.angel.ml.core.local

import com.tencent.angel.ml.core.conf.{MLConf, SharedConf}
import com.tencent.angel.ml.core.{Learner, Model}
import com.tencent.angel.ml.core.data.{DataBlock, DataReader, LabeledData}
import com.tencent.angel.ml.core.network.Graph
import com.tencent.angel.ml.core.optimizer.decayer.{StepSizeScheduler, WarmRestarts}
import com.tencent.angel.ml.core.utils.Callback.VoidType
import com.tencent.angel.ml.core.utils.ValidationUtils
import org.apache.commons.logging.{Log, LogFactory}

class LocalLearner(conf: SharedConf) extends Learner {
  private val LOG: Log = LogFactory.getLog(classOf[LocalLearner])

  // 1. initial model, model can be view as a proxy of graph
  override val model: LocalModel = new LocalModel(conf)
  override val graph: Graph = model.graph

  // 2. build network
  model.buildNetwork()

  // 3. init or load matrices
  private val modelPath: String = conf.get(MLConf.ML_LOAD_MODEL_PATH)
  private val actionType: String = conf.get(MLConf.ML_ACTION_TYPE, MLConf.DEFAULT_ML_ACTION_TYPE)
  private val env = new LocalEvnContext
  if (actionType.equalsIgnoreCase("train") && modelPath.isEmpty) {
    model.createMatrices(env)
    model.init(0)
  } else {
    model.loadModel(env, modelPath)
  }

  private val lr0 = SharedConf.learningRate
  override protected val ssScheduler: StepSizeScheduler = new WarmRestarts(lr0, lr0/100)

  override protected def trainOneEpoch(epoch: Int, iter: Iterator[Array[LabeledData]], numBatch: Int): Double = {
    var batchCount: Int = 0
    var loss: Double = 0.0

    while (iter.hasNext) {
      // LOG.info("start to feedData ...")
      graph.feedData(iter.next())

      // LOG.info("start to pullParams ...")
      graph.pullParams(epoch)

      // LOG.info("calculate to forward ...")
      loss = graph.calLoss() // forward
      // LOG.info(s"The training los of epoch $epoch batch $batchCount is $loss" )

      // LOG.info("calculate to backward ...")
      graph.calBackward() // backward

      // LOG.info("calculate and push gradient ...")
      graph.pushGradient() // pushgrad
      // waiting all gradient pushed

      // LOG.info("waiting for push barrier ...")
      // barrier(0, graph)
      graph.setLR(ssScheduler.next())
      // LOG.info("start to update ...")
      graph.update[VoidType](epoch * numBatch + batchCount, 1) // update parameters on PS


      // waiting all gradient update finished
      // LOG.info("waiting for update barrier ...")
      // barrier(0, graph)
      batchCount += 1

      LOG.info(s"epoch $epoch batch $batchCount is finished!")
    }

    loss
  }

  override def train(posTrainData: DataBlock[LabeledData], negTrainData: DataBlock[LabeledData], validationData: DataBlock[LabeledData]): Model = {
    val numBatch: Int = SharedConf.numUpdatePerEpoch
    val batchSize: Int = if (negTrainData == null) {
      (posTrainData.size() + numBatch - 1) / numBatch
    } else {
      (posTrainData.size() + negTrainData.size() + numBatch - 1) / numBatch
    }
    val numEpoch: Int = SharedConf.epochNum
    val batchData: Array[LabeledData] = new Array[LabeledData](batchSize)

    val iter: Iterator[Array[LabeledData]] = if (negTrainData == null) {
      DataReader.getBathDataIterator(posTrainData, batchData, numBatch)
    } else {
      DataReader.getBathDataIterator(posTrainData, negTrainData, batchData, numBatch)
    }

    var loss: Double = 0.0
    (0 until numEpoch).foreach { epoch =>
      preHook.foreach(func => func(graph))
      loss += trainOneEpoch(epoch, iter, epoch)
      postHook.foreach(func => func(graph))

      validate(epoch, validationData)
    }

    model
  }

  override protected def validate(epoch: Int, valiData: DataBlock[LabeledData]): Unit = {
    ValidationUtils.calMetrics(model.predict(valiData), graph.getLossLayer.getLossFunc)
  }

  override protected def barrier(graph: Graph): Unit = {}
}
