/*
 * Tencent is pleased to support the open source community by making Angel available.
 *
 * Copyright (C) 2017 THL A29 Limited, a Tencent company. All rights reserved.
 *
 * Licensed under the BSD 3-Clause License (the "License"); you may not use this file except in
 * compliance with the License. You may obtain a copy of the License at
 *
 * https://opensource.org/licenses/BSD-3-Clause
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 *
 */

package com.tencent.angel.ml.utils

import java.io.IOException
import java.util

import com.tencent.angel.conf.AngelConf
import com.tencent.angel.worker.task.TaskContext
import it.unimi.dsi.fastutil.doubles.DoubleArrayList
import org.apache.commons.logging.{Log, LogFactory}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FSDataOutputStream, FileSystem, Path}
import org.apache.hadoop.hdfs.DFSConfigKeys

import scala.collection.mutable

/**
  * A Algorithm friendly log to print Metrics, such as Loss, Auc... turing training process
  *
  * ConsoleLogger will print metrics directly into console
  *
  * DistributedLogger will print these metrics into a Sharable File System, so they can be access by other systems.
  */

trait Logger {
  def LOG_FORMAT = "%10.6e"

  val _names = new util.ArrayList[String]()
  val _values = new util.HashMap[String, DoubleArrayList]()

  def setNames(names: String*): Unit = ???

  def addValues(values: Double*): Unit = ???

  def add(nameValues: (String, Double)*): Unit = ???

}

class DistributeLog(conf: Configuration) extends Logger{
  val LOG: Log = LogFactory.getLog(classOf[DistributeLog])
  /** Index name list */
  private var names: util.List[String] = null
  /** File output stream */
  private var outputStream: FSDataOutputStream = null


  /**
    * Init
    *
    */
  @throws(classOf[IOException])
  def init {
    val flushLen: Int = conf.getInt(AngelConf.ANGEL_LOG_FLUSH_MIN_SIZE, AngelConf.DEFAULT_ANGEL_LOG_FLUSH_MIN_SIZE)
    conf.setInt(DFSConfigKeys.DFS_CLIENT_WRITE_PACKET_SIZE_KEY, flushLen)
    conf.setInt(DFSConfigKeys.DFS_BYTES_PER_CHECKSUM_KEY, flushLen)
    val pathStr: String = conf.get(AngelConf.ANGEL_LOG_PATH)
    if (pathStr == null) {
      throw new IOException("log directory is null. you must set " + AngelConf.ANGEL_LOG_PATH)
    }
    LOG.info("algorithm log output directory=" + pathStr)
    val path: Path = new Path(pathStr + "/log")
    val fs: FileSystem = path.getFileSystem(conf)
    if (fs.exists(path)) {
      fs.delete(path, true)
    }
    outputStream = fs.create(path, true)
  }

  /**
    * Write the index names to file
    *
    * @param names index name list
    */
  @throws(classOf[IOException])
  def setNames(names: util.List[String]) {
    this.names = names
    import scala.collection.JavaConversions._
    for (name <- names) {
      outputStream.write((name + "\t").getBytes)
    }
    outputStream.writeBytes("\n")
    outputStream.hflush
  }

  /**
    * Write index values to file
    *
    * @param values index name to value map
    */
  @throws(classOf[IOException])
  def writeLog(values: mutable.Map[String, Double]) {
    for (v <- values.values) {
      outputStream.write((v + "\t").getBytes())
    }
    outputStream.writeBytes("\n")
    outputStream.hflush
  }

  /**
    * Close file writter
    **/
  @throws(classOf[IOException])
  def close {
    if (outputStream != null) {
      outputStream.close
    }
  }
}

object DistributedLog {
  val loggers = mutable.MutableList.empty[DistributeLog]
  def apply(ctx: TaskContext) = {
    val logger = new DistributeLog(ctx.getConf)
    loggers += logger
    logger
  }

  def shutdownLogger = {
    for (logger: DistributeLog <- loggers) {
      logger.close
    }
  }

  {
    sys.addShutdownHook(shutdownLogger)
  }

}

class ConsoleLogger extends Logger {

}

