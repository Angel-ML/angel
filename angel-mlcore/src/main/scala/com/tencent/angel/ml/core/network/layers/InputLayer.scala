package com.tencent.angel.ml.core.network.layers

import com.tencent.angel.ml.core.network.Graph
import com.tencent.angel.ml.math2.matrix.Matrix


abstract class InputLayer(name: String, outputDim: Int)(implicit graph: Graph)
  extends Layer(name, outputDim) {
  graph.addInputLayer(this)

  override def forward(): Matrix = {
    if (graph.isMatrixInCache(forwardKey)) {
      graph.getMatrixFromCache(forwardKey)
    } else {
      val forwardValue: Matrix = doForward(placeHolder.getFeats)
      graph.put2Cache(forwardKey, forwardValue)
      forwardValue
    }
  }

  protected def doForward(input: Matrix): Matrix

  override def backward(layer: Layer): Matrix = {
    if (graph.isMatrixInCache(backwardKey)) {
      graph.getMatrixFromCache(backwardKey)
    } else {
      val gradInput = gatherGradInput()
      doBackward(placeHolder.getFeats, gradInput)

      graph.put2Cache(backwardKey, null.asInstanceOf[Matrix])

      null.asInstanceOf[Matrix]
    }
  }

  protected def doBackward(input: Matrix, gradInput: Matrix): Unit
}
