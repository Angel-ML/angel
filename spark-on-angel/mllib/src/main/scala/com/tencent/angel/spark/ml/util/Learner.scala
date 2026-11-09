package com.tencent.angel.spark.ml.util

/**
 * Created by kevinzwyou on 16-8-26.
 */
trait Learner {
  /**
   * input: 训练数据
   * testSet：测试数据，可以为null
   * modelPath: 模型保存路径
   */
  def train(input: String, testSet: String): Model

  /**
   * input: 预测数据
   * modelPath: 模型路径
   */
  def predict(input: String, output: String, model: Model)

  /**
   * modelPath: 模型路径
   */
  def loadModel(modelPath: String): Model

  def process(
               actionType: String,
               input: String,
               modelPath: String,
               testSet: String,
               output: String) {
    actionType match {
      case ActionType.TRAIN =>
        train(input, testSet).save(modelPath)

      case ActionType.PREDICT =>
        val model = loadModel(modelPath)
        predict(input, output, model)
    }
  }
}
