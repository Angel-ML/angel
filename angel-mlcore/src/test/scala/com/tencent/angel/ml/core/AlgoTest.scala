package com.tencent.angel.ml.core

import com.tencent.angel.ml.core.conf.{MLConf, SharedConf}
import com.tencent.angel.ml.core.data.{DataBlock, LabeledData}
import com.tencent.angel.ml.core.local.LocalLearner
import com.tencent.angel.ml.core.local.data.LocalDataReader
import org.scalatest.{BeforeAndAfter, FunSuite}

class AlgoTest extends FunSuite with BeforeAndAfter {
  var conf: SharedConf = _
  var taskDataBlock: DataBlock[LabeledData] = _
  var validDataBlock: DataBlock[LabeledData] = _

  before {
    conf = SharedConf.get()
    val jsonFile = "E:\\github\\fitzwang\\angel\\angel-mlcore\\src\\test\\jsons\\logreg.json"
    conf.set(MLConf.ML_JSON_CONF_FILE, jsonFile)
    conf.setJson()

    //----------------------------------------------------------------------------------------
    val reader = new LocalDataReader(conf)
    val iter = reader.sourceIter("E:\\github\\fitzwang\\angel\\data\\a9a\\a9a_123d_train.libsvm")
    val dataBlocks = reader.readData2(iter)
    taskDataBlock = dataBlocks._1
    validDataBlock = dataBlocks._1
  }

  test("LR") {
    val learner = new LocalLearner(conf)
    learner.train(taskDataBlock, validDataBlock)
    // learner.train()
  }


}
