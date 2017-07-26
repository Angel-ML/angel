package com.tencent.angel.example.quickstart

import com.tencent.angel.ml.MLRunner
import org.apache.hadoop.conf.Configuration

class QSLRRunner extends MLRunner{
  /**
    * Training job to obtain a model
    */
  override
  def train(conf: Configuration): Unit = {
    train(conf, new QSLRModel(conf), classOf[QSLRTrainTask])
  }

  override def incTrain(conf: Configuration): Unit = ???

  override def predict(conf: Configuration): Unit = ???
}
