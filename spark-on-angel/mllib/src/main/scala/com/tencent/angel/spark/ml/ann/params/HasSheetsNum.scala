package com.tencent.angel.spark.ml.ann.params

import org.apache.spark.ml.param.{IntParam, Params}

trait HasSheetsNum extends Params {
  /**
    * param for num of worker sheets
    *
    * @group param
    */
  final val sheetsNum = new IntParam(this, "SheetsNum", "param for num of worker sheets")
  
  /** @group getParam */
  final def getSheetsNum: Int = $(sheetsNum)
  
  setDefault(sheetsNum, 1)
  
  /** @group setParam */
  final def setSheetsNum(m: Int): this.type = set(sheetsNum, m)
}