package com.tencent.angel.ml.core.data

import com.tencent.angel.ml.core.conf.{MLCoreConf, SharedConf}
import com.tencent.angel.ml.math2.utils.LabeledData
import org.apache.commons.logging.LogFactory


abstract class DataReader(conf: SharedConf) {
  import DataReader.BlockType._

  private val LOG = LogFactory.getLog(classOf[DataReader])

  private val valiRat = SharedConf.validateRatio

  private val transLabel: TransLabel = TransLabel.get(
    conf.getString(MLCoreConf.ML_DATA_LABEL_TRANS, MLCoreConf.DEFAULT_ML_DATA_LABEL_TRANS),
    conf.getDouble(MLCoreConf.ML_DATA_LABEL_TRANS_THRESHOLD, MLCoreConf.DEFAULT_ML_DATA_LABEL_TRANS_THRESHOLD)
  )
  private val isTraining: Boolean = conf.getString(MLCoreConf.ML_ACTION_TYPE, MLCoreConf.DEFAULT_ML_ACTION_TYPE).toLowerCase match {
    case "train" | "inctrain" => true
    case _ => false
  }
  private val parser: DataParser = DataParser(
    SharedConf.indexRange, SharedConf.inputDataFormat,
    conf.getString(MLCoreConf.ML_DATA_SPLITOR, MLCoreConf.DEFAULT_ML_DATA_SPLITOR),
    SharedConf.modelType,
    conf.getBoolean(MLCoreConf.ML_DATA_HAS_LABEL, MLCoreConf.DEFAULT_ML_DATA_HAS_LABEL),
    isTraining, transLabel)

  def sourceIter(pathName: String): Iterator[String]

  def readData1(iter: Iterator[String]): DataBlock[LabeledData] = {
    val start = System.currentTimeMillis()
    val taskDataBlock: DataBlock[LabeledData] = getDataBlock(DataReader.getBlockType(conf))

    while (iter.hasNext) {
      val out = parser.parse(iter.next())
      if (out != null) {
          taskDataBlock.put(out)
      }
    }

    taskDataBlock.flush()

    val cost = System.currentTimeMillis() - start

    LOG.info(s"Thread[${Thread.currentThread().getId}] preprocessed ${taskDataBlock.size} samples, " +
      s"${taskDataBlock.size} for train, processing time is $cost"
    )

    taskDataBlock
  }

  def readData2(iter: Iterator[String]): (DataBlock[LabeledData], DataBlock[LabeledData]) = {
    val start = System.currentTimeMillis()

    val taskDataBlock: DataBlock[LabeledData] = getDataBlock(DataReader.getBlockType(conf))
    val validDataBlock: DataBlock[LabeledData] = getDataBlock(Memory)

    var count = 0
    val vali = Math.ceil(1.0 / valiRat).toInt

    while (iter.hasNext) {
      val out = parser.parse(iter.next())
      if (out != null) {
        if (count % vali == 0)
          validDataBlock.put(out)
        else {
          taskDataBlock.put(out)
        }

        count += 1
      }
    }

    taskDataBlock.flush()
    validDataBlock.flush()

    val cost = System.currentTimeMillis() - start

    LOG.info(s"Thread[${Thread.currentThread().getId}] preprocessed ${
      taskDataBlock.size + validDataBlock.size
    } samples, ${taskDataBlock.size} for train, " +
      s"${validDataBlock.size} for validation." +
      s" processing time is $cost"
    )

    (taskDataBlock, validDataBlock)
  }

  def readData3(iter: Iterator[String]): (DataBlock[LabeledData], DataBlock[LabeledData], DataBlock[LabeledData]) = {
    val start = System.currentTimeMillis()

    val validDataBlock: DataBlock[LabeledData] = getDataBlock(Memory)
    val posDataBlock: DataBlock[LabeledData] = getDataBlock(DataReader.getBlockType(conf))
    val negDataBlock: DataBlock[LabeledData] = getDataBlock(DataReader.getBlockType(conf))

    var count = 0
    val vali = Math.ceil(1.0 / valiRat).toInt

    while (iter.hasNext) {
      val out = parser.parse(iter.next())
      if (out != null) {
        if (count % vali == 0)
          validDataBlock.put(out)
        else if (out.getY > 0) {
          posDataBlock.put(out)
        } else {
          negDataBlock.put(out)
        }

        count += 1
      }
    }


    posDataBlock.flush()
    negDataBlock.flush()
    validDataBlock.flush()

    val cost = System.currentTimeMillis() - start

    LOG.info(s"Thread[${Thread.currentThread().getId}] preprocessed ${
      posDataBlock.size + negDataBlock.size + validDataBlock.size
    } samples, ${posDataBlock.size + negDataBlock.size} for train, " +
      s"${validDataBlock.size} for validation." +
      s" processing time is $cost"
    )

    (posDataBlock, negDataBlock, validDataBlock)
  }

  def getDataBlock(dbType: BlockType): DataBlock[LabeledData]
}

object DataReader {
  object BlockType extends Enumeration {
    type BlockType = Value
    val Memory, Disk, DiskAndMemory = Value
  }

  def getBlockType(conf: SharedConf): BlockType.BlockType = {
    val storageLevel = SharedConf.storageLevel

    if (storageLevel.equalsIgnoreCase("memory")) {
      BlockType.Memory
    } else if (storageLevel.equalsIgnoreCase("memory_disk")) {
      BlockType.DiskAndMemory
    } else {
      BlockType.Disk
    }
  }

  def getBathDataIterator(trainData: DataBlock[LabeledData],
                          batchData: Array[LabeledData], numBatch: Int): Iterator[Array[LabeledData]] = {
    trainData.resetReadIndex()
    assert(batchData.length > 1)

    new Iterator[Array[LabeledData]] {
      private var count = 0

      override def hasNext: Boolean = count < numBatch

      override def next(): Array[LabeledData] = {
        batchData.indices.foreach { i => batchData(i) = trainData.loopingRead() }
        count += 1
        batchData
      }
    }
  }

  def getBathDataIterator(posData: DataBlock[LabeledData],
                          negData: DataBlock[LabeledData],
                          batchData: Array[LabeledData], numBatch: Int): Iterator[Array[LabeledData]] = {
    posData.resetReadIndex()
    negData.resetReadIndex()
    assert(batchData.length > 1)

    new Iterator[Array[LabeledData]] {
      private var count = 0
      val posnegRatio: Double = SharedConf.posnegRatio()
      val posPreNum: Int = Math.max((posData.size() + numBatch - 1) / numBatch,
        batchData.length * posnegRatio / (1.0 + posnegRatio)).toInt

      val posNum: Int = if (posPreNum < 0.5 * batchData.length) {
        Math.max(1, posPreNum)
      } else {
        batchData.length / 2
      }
      val negNum: Int = batchData.length - posNum

      val posDropRate: Double = if (posNum * numBatch > posData.size()) {
        0.0
      } else {
        1.0 * (posData.size() - posNum * numBatch) / posData.size()
      }

      val negDropRate: Double = if (negNum * numBatch > negData.size()) {
        0.0
      } else {
        1.0 * (negData.size() - negNum * numBatch) / negData.size()
      }

      override def hasNext: Boolean = count < numBatch

      override def next(): Array[LabeledData] = {
        (0 until posNum).foreach { i =>
          if (posDropRate == 0) {
            batchData(i) = posData.loopingRead()
          } else {
            var flag = true
            while (flag) {
              val pos = posData.loopingRead()
              if (Math.random() > posDropRate) {
                batchData(i) = pos
                flag = false
              }
            }
          }
        }

        (0 until negNum).foreach { i =>
          if (negDropRate == 0) {
            batchData(i + posNum) = negData.loopingRead()
          } else {
            var flag = true
            while (flag) {
              val neg = negData.loopingRead()
              if (Math.random() > negDropRate) {
                batchData(i + posNum) = neg
                flag = false
              }
            }
          }
        }

        count += 1
        batchData
      }
    }
  }
}
