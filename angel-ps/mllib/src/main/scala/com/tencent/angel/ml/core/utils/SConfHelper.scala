package com.tencent.angel.ml.core.utils

import java.io.File

import com.tencent.angel.conf.AngelConf
import com.tencent.angel.ml.core.conf.SharedConf
import org.apache.hadoop.conf.Configuration

trait SConfHelper {

  def initConf(conf: Configuration): SharedConf = {
    val sharedConf = SharedConf.get()

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
        val json = JsonUtils.parseAndUpdateJson(validateFileName, sharedConf)
        sharedConf.setJson(json)
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
