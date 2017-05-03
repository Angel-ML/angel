package com.tencent.angel.spark.ml.common

/**
 * Learner is a trait for all supervised machine learning algorithm.
 */
trait Learner {

  /**
   *
   * @param input train data path, HDFS or local path.
   * @param validateSet validate data, if validateSet == null, it means train without validate data
   * @return trained Model
   */
  def train(input: String, validateSet: String): Model

  /**
   *
   * @param input predict data path, HDFS or local path.
   * @param output the path for predict result to save
   * @param model trained Model
   */
  def predict(input: String, output: String, model: Model)


  /**
   *
   * @param modelPath model path.
   * @return
   */
  def loadModel(modelPath: String): Model

  def process(
      actionType: String,
      input: String,
      modelPath: String,
      validateSet: String,
      output: String) {
    actionType match {
      case "train" =>
        train(input, validateSet).save(modelPath)

      case "predict" =>
        val model = loadModel(modelPath)
        predict(input, output, model)
    }
  }
}
