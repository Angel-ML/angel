package org.apache.spark

import org.apache.hadoop.conf.Configuration
import org.apache.spark.deploy.SparkHadoopUtil
object AngelSparkHadoopUtil {
  def getConfiguration(sparkConf: SparkConf): Configuration = {
    SparkHadoopUtil.get.newConfiguration(sparkConf)
  }
}
