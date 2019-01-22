package com.tencent.angel.ml.core.network.layers

import com.tencent.angel.ml.core.PredictResult
import com.tencent.angel.ml.core.network.Graph
import com.tencent.angel.ml.core.optimizer.loss.LossFunc
import com.tencent.angel.ml.core.utils.LayerKeys
import com.tencent.angel.ml.math2.matrix.Matrix
import org.json4s.JsonAST.{JField, JString}
import org.json4s.JsonDSL._


class LossLayer(name: String, inputLayer: Layer, val lossFunc: LossFunc)(implicit graph: Graph)
  extends Layer(name, 1) {

  def predict(): List[PredictResult] = {
    lossFunc.predict(forward(), graph)
  }

  def calLoss(): Double = {
    lossFunc.calLoss(forward(), graph)
  }

  override def forward(): Matrix = {
    if (graph.isMatrixInCache(forwardKey)) {
      graph.getMatrixFromCache(forwardKey)
    } else {
      val forwardValue: Matrix = inputLayer.forward()
      graph.put2Cache(forwardKey, forwardValue)
      forwardValue
    }
  }

  override def backward(layer: Layer): Matrix = {
    if (graph.isMatrixInCache(backwardKey)) {
      val inputs = getAllInputs
      if (inputs.isEmpty) {
        null.asInstanceOf[Matrix]
      } else if (inputs.size == 1) {
        val validateLayer = if (layer != null) {
          layer
        } else {
          getAllInputs.head
        }

        assert(isInput(validateLayer))
        val key = s"$backwardKey/${validateLayer.name}"
        graph.getMatrixFromCache(key)
      } else {
        assert(isInput(layer))
        val key = s"$backwardKey/${layer.name}"
        graph.getMatrixFromCache(key)
      }
    } else {
      val grad = lossFunc.calGrad(forward(), graph)
      graph.put2Cache(backwardKey, grad)

      grad
    }

  }

  override def toJson: JField = {
    val layerJson = (LayerKeys.typeKey -> s"${this.getClass.getSimpleName}") ~
      (LayerKeys.outputDimKey -> outputDim) ~
      (LayerKeys.inputLayerKey, JString(inputLayer.name)) ~
      (LayerKeys.lossFuncKey, lossFunc.toJson)


    JField(name, layerJson)
  }
}
