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
 */

package com.tencent.angel.master.data;

import org.apache.hadoop.conf.Configuration;

import com.tencent.angel.conf.AngelConfiguration;
import com.tencent.angel.master.app.AMContext;

/**
 * Just for test while there is no data.
 */
public class DummyDataSpliter extends DataSpliter {

  public DummyDataSpliter(AMContext context) {
    super(context);
  }

  @Override
  public int getSplitNum() {
    Configuration conf = context.getConf();
    int workergroupNumber =
        conf.getInt(AngelConfiguration.ANGEL_WORKERGROUP_NUMBER,
            AngelConfiguration.DEFAULT_ANGEL_WORKERGROUP_NUMBER);
    int taskNumInWorker =
        conf.getInt(AngelConfiguration.ANGEL_WORKER_TASK_NUMBER,
            AngelConfiguration.DEFAULT_ANGEL_WORKER_TASK_NUMBER);
    return workergroupNumber * taskNumInWorker;
  }

  @Override
  public String[] getSplitLocations(int index) {
    return new String[0];
  }
}
