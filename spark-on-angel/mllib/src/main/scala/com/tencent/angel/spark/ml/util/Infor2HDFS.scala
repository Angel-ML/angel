/*
 * Tencent is pleased to support the open source community by making Angel available.
 *
 * Copyright (C) 2017-2018 THL A29 Limited, a Tencent company. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in 
 * compliance with the License. You may obtain a copy of the License at
 *
 * https://opensource.org/licenses/Apache-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 *
 */


package com.tencent.angel.spark.ml.util


import java.io.IOException

import org.apache.commons.logging.LogFactory
import org.apache.hadoop.fs.{FSDataOutputStream, Path}
import org.apache.hadoop.util.Daemon
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.StreamingContext

object Infor2HDFS extends Serializable {

  @transient var outputStream: FSDataOutputStream = _

  // save loss information to hdfs each batch
  def saveLog2HDFS(message: String): Unit = {
    outputStream.write(message.getBytes)
    outputStream.writeBytes("\n")
    outputStream.hflush()
  }

  // init path which used to write log to hdfs
  def initLogPath(ssc: StreamingContext, logPath: String) = {

    val outputPath = new Path(logPath)
    val conf = ssc.sparkContext.hadoopConfiguration

    val fs = outputPath.getFileSystem(conf)
    if (fs.exists(outputPath)) {
      fs.delete(outputPath, true)
    }

    outputStream = fs.create(outputPath, true)

    // registered shutdown hook
    sys.ShutdownHookThread {
      println("this is the end")
      close(outputStream)
    }

    new Daemon(new LogFlusher(outputStream, conf.getLong("angel.streaming.log.flush.interval", 60 * 1000))).start()
  }

  // init path which used to write model to hdfs
  def saveModel2HDFS(modelPath: String, messages: Array[String]) = {

    val outputPath = new Path(modelPath)
    val sc = SparkSession.builder().getOrCreate().sparkContext
    val conf = sc.hadoopConfiguration

    val fs = outputPath.getFileSystem(conf)
    if (fs.exists(outputPath)) {
      fs.delete(outputPath, true)
    }

    val modelStream = fs.create(outputPath, true)

    messages.foreach { str =>
      // save the model to hdfs
      modelStream.write(str.getBytes)
      modelStream.writeBytes("\n")
    }
    modelStream.hflush()

    close(modelStream)

  }


  def close(stream: FSDataOutputStream) = {
    stream.close()
  }

  class LogFlusher(outputStream: FSDataOutputStream, interval: Long) extends Runnable {
    val LOG = LogFactory.getLog(classOf[LogFlusher])

    @volatile var stopped = false

    override def run(): Unit = {
      while (!stopped) {
        try {
          outputStream.hflush()
          Thread.sleep(interval)
        } catch {
          case e: IOException => {
            LOG.error("flush hdfs failed", e)
          }
          case e: InterruptedException => {
            LOG.error("flush hdfs interrupted", e)
            stopped = true
          }
        }
      }
    }

    override def toString: String = "hdfs log flusher"

    def stop(): Unit = {
      stopped = true
    }
  }

}
