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

package com.tencent.angel.ps.impl;

import com.tencent.angel.AngelDeployMode;
import com.tencent.angel.conf.AngelConf;
import com.tencent.angel.master.MasterProtocol;

import org.apache.hadoop.conf.Configuration;

/**
 * Context of parameter server.
 */
public class PSContext {
  private static PSContext context = new PSContext();
  private ParameterServer ps;

  private PSContext() {}

  public static PSContext get() {
    return context;
  }

  public void setPs(ParameterServer ps) {
    this.ps = ps;
  }

  public int getTaskNum() {
    return ps.getConf().getInt(AngelConf.ANGEL_TASK_ACTUAL_NUM, 1);
  }

  public MatrixPartitionManager getMatrixPartitionManager() {
    return ps.getMatrixPartitionManager();
  }

  public Configuration getConf() {
    return ps.getConf();
  }
  
  public AngelDeployMode getDeployMode() {
    String mode =
        ps.getConf().get(AngelConf.ANGEL_DEPLOY_MODE,
            AngelConf.DEFAULT_ANGEL_DEPLOY_MODE);

    if (mode.equals(AngelDeployMode.LOCAL.toString())) {
      return AngelDeployMode.LOCAL;
    } else {
      return AngelDeployMode.YARN;
    }
  }

  public SnapshotManager getSnapshotManager() {
    return ps.getSnapshotManager();
  }
  
  public MasterProtocol getMaster() {
    return ps.getMaster();
  }
}
