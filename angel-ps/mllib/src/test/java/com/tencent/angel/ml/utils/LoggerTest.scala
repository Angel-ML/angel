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

import com.tencent.angel.conf.AngelConfiguration
import org.apache.hadoop.conf.Configuration
import org.apache.log4j.PropertyConfigurator
import org.junit.Test

class LoggerTest {

  PropertyConfigurator.configure("../conf/log4j.properties")

  @Test
  def TestAdd() {
    PropertyConfigurator.configure("../conf/log4j.properties")

    val logPath = "./src/test/log1"

    val conf = new Configuration()
    conf.set(AngelConfiguration.ANGEL_LOG_PATH, logPath)
    //val log = DistributedLogger(c)

//    log.setNames("a", "b", "c")
//    log.add(1, 2, 3)
//    log.add(4, 5, 6)
//    log.add(7, 8, 9)

  }

  @Test
  def TestAddValues() {
    PropertyConfigurator.configure("../log4j.properties")

    val logPath = "./src/test/log2"

    val conf = new Configuration()
    conf.set(AngelConfiguration.ANGEL_LOG_PATH, logPath)
//    val log = DistributedLogger(conf)
//
//    log.setNames("d", "e", "f")
//    log.addValues(("d", 10), ("e", 20), ("f", 30))
//    log.addValues(("d", 40), ("e", 50), ("f", 60))
//    log.addValues(("d", 70), ("e", 80), ("f", 90))

  }
}