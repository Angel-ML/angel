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
 * Unless required by applicable law or agreed to in writing, software distributed under the License is
 * distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied. See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.tencent.angel.ml.math.factory;

import com.tencent.angel.conf.AngelConfiguration;
import com.tencent.angel.psagent.PSAgentContext;

import org.apache.commons.pool.impl.GenericObjectPool;
import org.apache.hadoop.conf.Configuration;

public class TMatrixFactory {

  @SuppressWarnings({"rawtypes", "unchecked"})
  public static GenericObjectPool initCOOIntMatrixPool(int row, int col) {
    Configuration conf = PSAgentContext.get().getConf();
    int taskNumber =
        conf.getInt(AngelConfiguration.ANGEL_TASK_ACTUAL_NUM, conf.getInt(
            AngelConfiguration.ANGEL_WORKER_TASK_NUMBER,
            AngelConfiguration.DEFAULT_ANGEL_WORKER_TASK_NUMBER));

    GenericObjectPool.Config config = new GenericObjectPool.Config();
    config.maxActive = taskNumber * 3;
    config.maxWait = 1000;
    config.maxIdle = taskNumber;
    config.minIdle = 0;
    config.testOnBorrow = false;
    config.testOnReturn = false;
    config.minEvictableIdleTimeMillis = 10000;
    config.whenExhaustedAction = GenericObjectPool.WHEN_EXHAUSTED_BLOCK;
    GenericObjectPool pool =
        new GenericObjectPool(
            new TMatrixPoolableObjectFactory(row, col, new COOIntMatrixBuilder()), config);
    return pool;
  }
}
