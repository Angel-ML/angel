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

package com.tencent.angel.ml.toolkits.modelconverter

import com.tencent.angel.worker.task.{BaseTask, TaskContext}
import org.apache.commons.logging.{Log, LogFactory}
import org.apache.hadoop.io.{LongWritable, Text}

/*
 * ModelConverterTask convert the binary PSModel output files to plain text
 */
class ModelConverterTask(val ctx: TaskContext) extends BaseTask[LongWritable, Text, Text](ctx) {
  val LOG: Log = LogFactory.getLog(classOf[ModelConverterTask])

  override
  def parse(key: LongWritable, value: Text): Text = {
    // Do nothing
    null
  }

  override
  def preProcess(ctx: TaskContext) {
    // Do nothing in the preprocess function
  }

  override def run(ctx: TaskContext): Unit = {
    val parser = new ModelConverter(ctx.getConf)
    parser.parse()
  }
}
