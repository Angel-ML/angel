package com.tencent.angel.ml.core.network

import com.tencent.angel.ml.core.PredictResult
import com.tencent.angel.ml.core.conf.SharedConf
import com.tencent.angel.ml.core.network.layers.{Trainable, _}
import com.tencent.angel.ml.core.optimizer.loss.LossFunc
import com.tencent.angel.ml.core.utils.JsonUtils.{J2Pretty, layer2Json}
import com.tencent.angel.ml.core.utils.{DataCache, MLException, TimeStats}
import com.tencent.angel.ml.core.variable.VariableProvider
import com.tencent.angel.ml.servingmath2.matrix.Matrix
import com.tencent.angel.ml.servingmath2.utils.LabeledData
import com.tencent.angel.ml.servingmath2.vector.Vector
import org.apache.commons.logging.{Log, LogFactory}
import org.json4s.JsonAST.{JField, JObject}

import scala.collection.mutable
import scala.collection.mutable.ListBuffer


class Graph(val provider: VariableProvider, val conf: SharedConf, val taskNum: Int){
  private val LOG: Log = LogFactory.getLog(classOf[Graph])

  protected val inputLayers = new ListBuffer[InputLayer]()
  protected var lossLayer: LossLayer = _
  protected val trainableLayer = new ListBuffer[Trainable]()
  protected val dataCache: DataCache = new DataCache()
  private var batchSize: Int = -1
  val placeHolder = new PlaceHolder(conf)

  val timeStats = new TimeStats()

  def normalFactor: Double = {
    if (batchSize == -1) {
      throw MLException("batchSize not set, feed data will set batchSize automatic")
    }

    1.0 / (batchSize * taskNum)
  }

  protected var lr: Double = conf.learningRate

  def addInputLayer(layer: InputLayer): this.type = {
    inputLayers.append(layer)
    this
  }

  def getALLInputLayers: List[InputLayer] = inputLayers.toList

  def getInputLayer(name: String): InputLayer = {
    val layerOption = inputLayers.collectFirst {
      case layer: InputLayer if layer.name == name => layer
    }

    layerOption.getOrElse(null.asInstanceOf[InputLayer])
  }

  def setLossLayer(layer: LossLayer): this.type = {
    lossLayer = layer
    this
  }

  def getLossLayer: LossLayer = lossLayer

  def getLossFunc: LossFunc = lossLayer.lossFunc

  def addTrainableLayer(layer: Trainable): this.type = {
    trainableLayer.append(layer)
    this
  }

  def getALLTrainableLayers: List[Trainable] = {
    trainableLayer.toList
  }

  def getTrainableLayer(name: String): Trainable = {
    val trainableOption = trainableLayer.collectFirst {
      case layer: Layer if layer.name == name => layer.asInstanceOf[Trainable]
    }

    trainableOption.getOrElse(null.asInstanceOf[Trainable])
  }

  protected def deepFirstDown(layer: Layer)(predicate: Layer => Boolean, action: Layer => Unit): Unit = {
    if (predicate(layer)) {
      action(layer)
      layer match {
        case l: JoinLayer =>
          l.inputLayers.foreach { lowerLayer =>
            deepFirstDown(lowerLayer)(predicate, action)
          }
        case l: LinearLayer => deepFirstDown(l.inputLayer)(predicate, action)
        case _: InputLayer =>
      }
    }
  }


  /** **********************************************************************************
    * training
    */

  def setLR(lr: Double): this.type = {
    this.lr = lr
    trainableLayer.foreach { trainable =>
      trainable.optimizer.setLR(lr)
    }

    this
  }

  def getLR: Double = this.lr

  def getBatchSize: Int = {
    if (batchSize == -1) {
      throw MLException("batchSize not set, please feed data first!")
    }

    batchSize
  }

  def feedData(data: Array[LabeledData]): this.type = {
    batchSize = data.length
    placeHolder.feedData(data)

    this
  }

  // forward
  def calForward(): Double = {
    val start = System.currentTimeMillis()
    clearCache()
    val loss = lossLayer.calLoss()
    val end = System.currentTimeMillis()

    timeStats.forwardTime += end - start

    loss
  }

  // backward
  def calBackward(): Unit = {
    val start = System.currentTimeMillis()
    inputLayers.foreach { layer => layer.backward(layer) }
    val end = System.currentTimeMillis()

    timeStats.backwardTime += end - start
  }


  /** **********************************************************************************
    * predict
    */

  def predict(): List[PredictResult] = {
    val start = System.currentTimeMillis()
    clearCache()
    val res = lossLayer.predict()
    val end = System.currentTimeMillis()
    timeStats.predictTime += end - start

    res
  }

  /** **********************************************************************************
    * Matrix Cache
    */

  def put2Cache(name: String, mat: Matrix): this.type = {
    dataCache.addMatrix(name, mat)
    this
  }

  def put2Cache(name: String, vec: Vector): this.type = {
    dataCache.addVector(name, vec)
    this
  }

  def isMatrixInCache(name: String): Boolean = {
    dataCache.hasMatrix(name)
  }

  def isVectorInCache(name: String): Boolean = {
    dataCache.hasVector(name)
  }

  def getMatrixFromCache(name: String): Matrix = {
    dataCache.getMatrix(name)
  }

  def getVectorFromCache(name: String): Vector = {
    dataCache.getVector(name)
  }

  def clearCache(): this.type = {
    dataCache.clearAll()
    this
  }

  /** **********************************************************************************
    * toString/toJson
    */

  override def toString: String = {
    val str = new StringBuilder
    deepFirstDown(lossLayer.asInstanceOf[Layer])(_ => true, layer => str.append(layer.toString + "\n"))
    str.toString()
  }

  def toJson: String = {
    implicit val jsonMap: mutable.HashMap[String, JField] = new mutable.HashMap[String, JField]()
    layer2Json(lossLayer.asInstanceOf[Layer])
    J2Pretty(JObject(jsonMap.values.toList))
  }
}
