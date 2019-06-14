package com.tencent.angel.ml.core.local.data

import java.io.IOException
import java.util
import java.util.Collections

import com.tencent.angel.ml.core.data.DataBlock
import com.tencent.angel.ml.math2.utils.LabeledData
import org.apache.commons.logging.{Log, LogFactory}
import org.ehcache.sizeof.SizeOf

class LocalMemoryDataBlock(initSize: Int, maxUseMemroy: Long) extends DataBlock[LabeledData] {
  private val LOG: Log = LogFactory.getLog(classOf[LocalMemoryDataBlock])

  private var estimateSampleNumber: Int = 100
  val initCapacity = if (initSize > 0) {
    estimateSampleNumber = initSize
    initSize
  } else {
    estimateSampleNumber
  }
  private val vList = new util.ArrayList[LabeledData]()
  private var isFull: Boolean = false

  @throws[IOException]
  override def read(): LabeledData = {
    if (readIndex < writeIndex) {
      val value = vList.get(readIndex)
      readIndex += 1
      value
    } else {
      null.asInstanceOf[LabeledData]
    }
  }

  @throws[IOException]
  override protected def hasNext: Boolean = readIndex < writeIndex

  @throws[IOException]
  override def get(index: Int): LabeledData = {
    if (index < 0 || index >= writeIndex) {
      throw new IOException("index not in range[0," + writeIndex + ")")
    }

    vList.get(index)
  }

  @throws[IOException]
  override def put(value: LabeledData): Unit = {
    if (writeIndex < estimateSampleNumber) {
      vList.add(value)
      writeIndex += 1
      if (writeIndex == estimateSampleNumber && !isFull) {
        estimateAndResizeVList()
      }
    } else {
      LOG.info("Over maxUseMemroy, No value added!")
    }
  }

  override def resetReadIndex(): Unit = {
    readIndex = 0
  }

  override def clean(): Unit = {
    readIndex = 0
    writeIndex = 0
    vList.clear()
  }

  override def shuffle(): Unit = Collections.shuffle(vList)

  override def flush(): Unit = {}

  override def slice(startIndex: Int, length: Int): DataBlock[LabeledData] = ???

  private def estimateAndResizeVList(): Unit = {
    val avgDataItemSize = (SizeOf.newInstance().deepSizeOf(vList) + vList.size -1) / vList.size
    val maxStoreNum = (maxUseMemroy / avgDataItemSize).toInt

    val capacity = if (maxStoreNum < 2 * vList.size) {
      isFull = true
      maxStoreNum
    } else {
      2 * vList.size
    }

    estimateSampleNumber = (0.8 * capacity).toInt

    vList.ensureCapacity(capacity)
    LOG.debug("estimate sample number=" + vList.size + ", avgDataItemSize=" + avgDataItemSize +
      ", maxStoreNum=" + maxStoreNum + ", maxUseMemroy=" + maxUseMemroy)
  }
}
