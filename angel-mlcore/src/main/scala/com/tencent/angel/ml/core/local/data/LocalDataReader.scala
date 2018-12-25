package com.tencent.angel.ml.core.local.data

import com.tencent.angel.ml.core.conf.SharedConf
import com.tencent.angel.ml.core.data.DataReader.BlockType.BlockType
import com.tencent.angel.ml.core.data.{DataBlock, DataReader, LabeledData}

import scala.io.Source

class LocalDataReader(conf: SharedConf) extends DataReader(conf) {
  def sourceIter(pathName: String): Iterator[String] = {
    val source = Source.fromFile(pathName, "UTF-8")
    source.getLines()
  }

  override def getDataBlock(dbType: BlockType): DataBlock[LabeledData] = {
    new LocalMemoryDataBlock(-1, 1000* 1024 * 1024)
  }
}
