package com.tencent.angel.ml.classification.lr

import com.tencent.angel.ml.conf.MLConf
import com.tencent.angel.ml.feature.LabeledData
import com.tencent.angel.ml.math.TVector
import com.tencent.angel.ml.math.vector.{SparseLongKeyDummyVector, SparseLongKeySortedDoubleVector}
import com.tencent.angel.ml.task.TrainTask
import com.tencent.angel.ml.utils.DataParser64
import com.tencent.angel.worker.storage.MemoryDataBlock
import com.tencent.angel.worker.task.TaskContext
import it.unimi.dsi.fastutil.longs.Long2IntOpenHashMap
import org.apache.commons.logging.{Log, LogFactory}
import org.apache.hadoop.io.{LongWritable, Text}

class LRTrain64Task(val ctx: TaskContext) extends TrainTask[LongWritable, Text](ctx){
  val LOG: Log = LogFactory.getLog(classOf[LRTrain64Task])

  // feature number of training data
  private val feaNum: Long = conf.getLong(MLConf.ML_FEATURE_NUM, MLConf.DEFAULT_ML_FEATURE_NUM)
  // data format of training data, libsvm or dummy
  private val dataFormat = conf.get(MLConf.ML_DATA_FORMAT, "dummy")
  // validate sample ratio
  private val valiRat = conf.getDouble(MLConf.ML_VALIDATE_RATIO, 0.05)

  private val dataParser = DataParser64(dataFormat, feaNum, true)

  // validation data storage
  var validDataBlock = new MemoryDataBlock[LabeledData](-1)

  // Enable index get
  private val enableIndexGet = conf.getBoolean(MLConf.ML_INDEX_GET_ENABLE, MLConf.DEFAULT_ML_INDEX_GET_ENABLE)

  var indexes:Array[Long] = new Array[Long](0)

  override
  def train(ctx: TaskContext) {
    val trainer = new LRLearner(ctx)
    trainer.train(taskDataBlock, validDataBlock, indexes)
  }

  /**
    * parse the input text to trainning data
    *
    * @param key   the key
    * @param value the text
    */
  override
  def parse(key: LongWritable, value: Text): LabeledData = {
    dataParser.parse(value.toString())
  }

  /**
    * before trainning, preprocess input text to trainning data and put them into trainning data
    * storage and validation data storage separately
    */
  override
  def preProcess(taskContext: TaskContext) {
    val start = System.currentTimeMillis()

    var count = 0
    val vali = Math.ceil(1.0 / valiRat).asInstanceOf[Int]

    val reader = taskContext.getReader
    val indexSet = new Long2IntOpenHashMap()
    while (reader.nextKeyValue) {
      val out = parse(reader.getCurrentKey, reader.getCurrentValue)
      if (out != null) {
        updateIndex(out, indexSet)
        if (count % vali == 0)
          validDataBlock.put(out)
        else
          taskDataBlock.put(out)
        count += 1
      }
    }
    if(enableIndexGet && !indexSet.isEmpty) {
      indexes = indexSet.keySet().toLongArray
      LOG.info("after preprocess data , index length = " + indexes.length)
    }

    taskDataBlock.flush()
    validDataBlock.flush()

    val cost = System.currentTimeMillis() - start
    LOG.info(s"Task[${ctx.getTaskIndex}] preprocessed ${taskDataBlock.size +
      validDataBlock.size} samples, ${taskDataBlock.size} for train, " +
      s"${validDataBlock.size} for validation. feanum=$feaNum")
  }

  def updateIndex(labeledData: LabeledData, indexSet:Long2IntOpenHashMap): Unit = {
    if(enableIndexGet) {
      val x:TVector = labeledData.getX
      if(x.isInstanceOf[SparseLongKeyDummyVector]) {
        updateIndex(x.asInstanceOf[SparseLongKeyDummyVector].getIndices, indexSet)
      } else if(x.isInstanceOf[SparseLongKeySortedDoubleVector]) {
        updateIndex(x.asInstanceOf[SparseLongKeySortedDoubleVector].getIndexes, indexSet)
      }
    }
  }

  def updateIndex(itemIndexes:Array[Long], indexSet:Long2IntOpenHashMap): Unit = {
    for(index <- itemIndexes) {
      indexSet.put(index, 0)
    }
  }
}
