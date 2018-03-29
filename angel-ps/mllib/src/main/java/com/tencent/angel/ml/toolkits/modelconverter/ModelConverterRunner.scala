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

import com.tencent.angel.AppSubmitter
import com.tencent.angel.client.AngelClientFactory
import com.tencent.angel.conf.AngelConf
import org.apache.hadoop.conf.Configuration

/**
  * Run model convert task use cluster mode. Normally, use the "ModelConvert" please, which is in the tools package.
  * If your client machine doesn't have enough resources, you can start the Angel job to complete the model convert.
  */
class ModelConverterRunner extends AppSubmitter {

  /**
    * Run model convert task
    *
    * @param conf : configuration of algorithm and resource
    */
  override def submit(conf: Configuration): Unit = {
    // For parse model task, we only start one task.
    conf.setInt(AngelConf.ANGEL_WORKERGROUP_NUMBER, 1)
    conf.setInt(AngelConf.ANGEL_WORKER_TASK_NUMBER, 1)
    conf.setInt(AngelConf.ANGEL_PS_NUMBER, 1)
    conf.setInt(AngelConf.ANGEL_PS_MEMORY_GB, 2)
    conf.setInt(AngelConf.ANGEL_PS_CPU_VCORES, 1)
    conf.setBoolean(AngelConf.ANGEL_AM_USE_DUMMY_DATASPLITER, true)

    val client = AngelClientFactory.get(conf)
    client.startPSServer()
    client.runTask(classOf[ModelConverterTask])
    client.waitForCompletion()
    client.stop()
  }
}
