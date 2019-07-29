package com.tencent.angel.ml.core.utils

import java.io.File

import com.tencent.angel.conf.AngelConf
import com.tencent.angel.mlcore.conf.SharedConf
import com.tencent.angel.mlcore.utils.JsonUtils
import org.apache.hadoop.conf.Configuration

trait SConfHelper {

  def initConf(conf: Configuration): SharedConf = {
    val sharedConf = new SharedConf

    // 1. parse json and update conf
    if (conf.get(AngelConf.ANGEL_ML_CONF) != null) {
      var jsonFileName = conf.get(AngelConf.ANGEL_ML_CONF)
      val validateFileName = if (new File(jsonFileName).exists()) {
        jsonFileName
      } else {
        val splits = jsonFileName.split(File.separator)
        jsonFileName = splits(splits.length - 1)

        val file = new File(jsonFileName)
        if (file.exists()) {
          jsonFileName
        } else {
          println("File not found! ")
          ""
        }
      }

      if (!validateFileName.isEmpty) {
        JsonUtils.parseAndUpdateJson(validateFileName, sharedConf, conf)
      }
    }

    // 2. add configure on Hadoop Configuration
    val iter = conf.iterator()
    while (iter.hasNext) {
      val entry = iter.next()
      val key = entry.getKey
      val value = entry.getValue

      if (key.startsWith("ml.") || key.startsWith("angel.")) {
        sharedConf.set(key, value)
      }
    }

    sharedConf
  }

}
