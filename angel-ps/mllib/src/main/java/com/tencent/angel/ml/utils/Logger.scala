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

import java.util
import java.util.function.ToIntFunction
import java.util.{Comparator, Map}

import com.tencent.angel.conf.AngelConfiguration
import com.tencent.angel.exception.AngelException
import com.tencent.angel.worker.task.TaskContext
import it.unimi.dsi.fastutil.doubles.DoubleArrayList
import org.apache.hadoop.fs.{FSDataOutputStream, FileSystem, Path}

import scala.collection.JavaConversions._

/**
  * A Algorithm friendly log to print Metrics, such as Loss, Auc... turing training process
  *
  * ConsoleLogger will print metrics directly into console
  *
  * DistributedLogger will print these metrics into a Sharable File System, so they can be access by other systems.
  */

trait Logger {
  val _names = new util.ArrayList[String]()
  val _values = new util.HashMap[String, DoubleArrayList]()

  def setNames(names: String*): Unit = ???

  def addValues(values: Double*): Unit = ???

  def add(nameValues: (String, Double)*): Unit = ???

}

class ConsoleLogger extends Logger {

}

class DistributedLogger(ctx: TaskContext) extends Logger {
  var form = "%10.6e"

  val conf = ctx.getConf
  val pathStr = conf.get(AngelConfiguration.ANGEL_LOG_PATH)

  if (pathStr == null) {
    throw new AngelException("log directory is null. you must set " + AngelConfiguration
      .ANGEL_SAVE_MODEL_PATH )
  }

  val path = new Path(pathStr + "/log_" + ctx.getTaskIndex.toString)
  var fs: FileSystem = path.getFileSystem(conf)
  if (fs.exists(path)) {
    fs.delete(path, true)
  }

  var out: FSDataOutputStream = fs.create(path, true)

  var closed = false

  override
  def setNames(names: String*): Unit = {
    for (name <- names.toArray) {
      _names.add(name)
      _values.put(name, new DoubleArrayList)
      out.writeBytes(name + "\t")
    }
    out.writeBytes("\n")
  }

  override
  def addValues(values: Double*): Unit = {
    assert(values.size == _names.size())
    for (v <- values) {
      out.writeBytes(form.format(v) + "\t")
    }
    out.writeBytes("\n")
  }

  override
  def add(nameValues: (String, Double)*): Unit = {
    for ((n, v) <- nameValues) {
      _values.get(n).add(v)
    }
    val longest = _values.entrySet().stream().sorted(Comparator.comparingInt(
      new ToIntFunction[Map.Entry[String, DoubleArrayList]]() {
        override def applyAsInt(value: Map.Entry[String, DoubleArrayList]): Int = {
          value.getValue.size()
        }
      }).reversed()).findFirst().get().getKey

    for (i <- 0 until _values.get(longest).size()) {
      for (name <- _names.toArray()) {
        val _value = _values.get(name);
        val v = if (!_value.isEmpty && _value.size() > i) _value.getDouble(i) else 0.0d
        out.writeBytes(form.format(v) + "\t")
      }
      out.writeBytes("\n")
    }
    _values.clear()
  }


  def close(): Unit = {
    if (!closed) {
      out.close()
      fs = null
      out = null
      closed = true
    }
  }
}


object DistributedLogger {
  val loggers = new util.ArrayList[DistributedLogger]()

  {
    sys.addShutdownHook(shutdownLogger)
  }

  def shutdownLogger = {
    for (logger: DistributedLogger <- loggers) {
      logger.close
    }
  }

  def apply(ctx: TaskContext) = {
    val logger = new DistributedLogger(ctx)
    loggers.add(logger)
    logger
  }
}
