package com.tencent.angel.ml.core

import com.tencent.angel.ml.core.conf.{MLConf, SharedConf}
import com.tencent.angel.ml.core.data.{DataBlock, LabeledData}
import com.tencent.angel.ml.core.local.LocalLearner
import com.tencent.angel.ml.core.local.data.LocalDataReader
import org.scalatest.{BeforeAndAfter, FunSuite}

class AlgoTest extends FunSuite with BeforeAndAfter {
  var conf: SharedConf = _
  var reader: LocalDataReader = _
  var taskDataBlock: DataBlock[LabeledData] = _
  var posDataBlock: DataBlock[LabeledData] = _
  var negDataBlock: DataBlock[LabeledData] = _
  var validDataBlock: DataBlock[LabeledData] = _

  def init1(jsonFile: String, sourceFile: String): Unit = {
    conf = SharedConf.get()
    conf.set(MLConf.ML_JSON_CONF_FILE, jsonFile)
    conf.setJson()

    reader = new LocalDataReader(conf)
    val iter = reader.sourceIter(sourceFile)
    val dataBlocks = reader.readData2(iter)
    taskDataBlock = dataBlocks._1
    validDataBlock = dataBlocks._2

    taskDataBlock.shuffle()
  }

  def init2(jsonFile: String, sourceFile: String): Unit = {
    conf = SharedConf.get()
    conf.set(MLConf.ML_JSON_CONF_FILE, jsonFile)
    conf.setJson()

    reader = new LocalDataReader(conf)
    val iter = reader.sourceIter(sourceFile)
    val dataBlocks = reader.readData3(iter)
    posDataBlock = dataBlocks._1
    negDataBlock = dataBlocks._2
    validDataBlock = dataBlocks._3
  }

  def train1(): Unit = {
    val learner = new LocalLearner(conf)
    // learner.train(taskDataBlock, validDataBlock)
    // learner.model.predict(validDataBlock).foreach(res => println(res.getText))
    println(learner.model.predict(validDataBlock.get(0)).getText)
  }

  def train2(): Unit = {
    val learner = new LocalLearner(conf)
    learner.train(posDataBlock, negDataBlock, validDataBlock)
  }

  test("LR") {
    val jsonFile = "E:\\github\\fitzwang\\angel\\angel-mlcore\\src\\test\\jsons\\logreg.json"
    val sourceFile = "E:\\github\\fitzwang\\angel\\data\\a9a\\a9a_123d_train.dummy"

    init2(jsonFile, sourceFile)
    train2()
  }

  test("Softmax") {
    val jsonFile = "E:\\github\\fitzwang\\angel\\angel-mlcore\\src\\test\\jsons\\softmax.json"
    val sourceFile = "E:\\github\\fitzwang\\angel\\data\\protein\\protein_357d_train.libsvm"

    init1(jsonFile, sourceFile)
    train1()
  }

  test("DNN") {
    val jsonFile = "E:\\github\\fitzwang\\angel\\angel-mlcore\\src\\test\\jsons\\dnn.json"
    val sourceFile = "E:\\github\\fitzwang\\angel\\data\\census\\census_148d_train.libsvm"

    init1(jsonFile, sourceFile)
    train1()
  }

  test("DAW") {
    val jsonFile = "E:\\github\\fitzwang\\angel\\angel-mlcore\\src\\test\\jsons\\daw.json"
    val sourceFile = "E:\\github\\fitzwang\\angel\\data\\census\\census_148d_train.dummy"

    init2(jsonFile, sourceFile)
    train2()
  }
}
