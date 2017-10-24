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

package com.tencent.angel.localcluster;

import com.tencent.angel.common.Location;
import com.tencent.angel.ps.PSAttemptId;
import com.tencent.angel.ps.impl.PSContext;
import com.tencent.angel.ps.impl.ParameterServer;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;

/**
 * Local Angel PS. It startups the {@link ParameterServer} using a thread.
 */
public class LocalPS extends Thread {
  private static final Log LOG = LogFactory.getLog(LocalPS.class);
  /**ps*/
  private final ParameterServer ps;
  
  /**
   * Create a local ps
   * @param psAttemptId ps attempt id
   * @param masterLocation master location
   * @param conf cluster configuration
   */
  public LocalPS(PSAttemptId psAttemptId, Location masterLocation, Configuration conf) {
    ps = new ParameterServer(psAttemptId.getParameterServerId().getIndex(), psAttemptId.getIndex(), masterLocation.getIp(), masterLocation.getPort(), conf);
    PSContext.get().setPs(ps);
  }

  @Override
  public void run() {
    try {
      ps.initialize();
      ps.start();
    } catch (Exception e) {
      LOG.fatal("ps " + ps.getPSAttemptId() + " start failed.", e);
      ps.failed(e.getMessage());
    }   
  }

  /**
   * Get ps
   * @return ps
   */
  public ParameterServer getPS() {
    return ps;
  }
  
  /**
   * Exit
   */
  public void exit() {
    ps.stop(0);
    interrupt();
  }
}
