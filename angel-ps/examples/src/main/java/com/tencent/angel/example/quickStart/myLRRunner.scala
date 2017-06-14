package com.tencent.angel.example.quickStart

import com.tencent.angel.ml.MLRunner
import com.tencent.angel.ml.classification.lr.LRTrainTask
import org.apache.hadoop.conf.Configuration
import com.tencent.angel.example.quickStart.myLRModel

class myLRRunner extends MLRunner{
  /**
    * Training job to obtain a model
    */
  override
  def train(conf: Configuration): Unit = {
    train(conf, new myLRModel(null, conf), classOf[myLRTrainTask])
  }

  override def incTrain(conf: Configuration): Unit = ???

  override def predict(conf: Configuration): Unit = ???
}
