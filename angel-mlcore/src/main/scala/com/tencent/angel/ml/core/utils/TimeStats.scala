package com.tencent.angel.ml.core.utils
import org.apache.commons.logging.{Log, LogFactory}

class TimeStats(
                 var forwardTime: Long = 0,
                 var backwardTime: Long = 0,
                 var pullParamsTime: Long = 0,
                 var pushParamsTime: Long = 0,
                 var updateTime: Long = 0,
                 var createTime: Long = 0,
                 var initTime: Long = 0,
                 var loadTime: Long = 0,
                 var saveTime: Long = 0,
                 var predictTime: Long = 0
               ) extends Serializable {
  val LOG: Log = LogFactory.getLog(classOf[TimeStats])

  def summary(): String = {
    val summaryString = s"\nSummary: \n\t" +
      s"forwardTime = $forwardTime, \n\tbackwardTime = $backwardTime, \n\t" +
      s"pullParamsTime = $pullParamsTime, \n\tcreateTime = $createTime, \n\t" +
      s"initTime = $initTime, \n\tloadTime = $loadTime, \n\t" +
      s"saveTime = $saveTime, \n\tpredictTime = $predictTime, \n\t" +
      s"pushParamsTime = $pushParamsTime, \n\tupdateTime = $updateTime"

    LOG.info(summaryString)
    summaryString
  }
}