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

package com.tencent.angel.psagent.matrix.transport;

import com.tencent.angel.ml.matrix.transport.PSFailedReport;
import com.tencent.angel.ml.matrix.transport.PSLocation;
import com.tencent.angel.psagent.PSAgentContext;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.util.HashMap;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Report ps network errors to Master
 */
public class PSAgentPSFailedReporter {
  private static final Log LOG = LogFactory.getLog(PSAgentPSFailedReporter.class);
  private final PSFailedReport report;
  private final PSAgentContext context;
  private final AtomicBoolean stopped;
  private final int reportIntervalMs = 5000;
  private Thread reporter;

  public PSAgentPSFailedReporter(PSAgentContext context) {
    this.context = context;
    this.stopped = new AtomicBoolean(false);
    this.report = new PSFailedReport();
  }

  public void init() {

  }

  public void start() {
    reporter = new Thread(()->{
      while (!stopped.get() && !Thread.interrupted()) {
        try {
          Thread.sleep(reportIntervalMs);
          HashMap<PSLocation, Integer> counters = report.getReports();
          if(counters.isEmpty()) {
            continue;
          }
          context.getMasterClient().psFailedReport(counters);
        } catch (Exception e) {
          if(!stopped.get()) {
            LOG.error("report ps failed failed ", e);
          }
        }
      }
    });

    reporter.setName("psfailed-reporter");
    reporter.start();
  }

  public void stop() {
    if(!stopped.getAndSet(true)) {
      if(reporter != null) {
        reporter.interrupt();
      }
    }
  }

  public void psFailed(PSLocation psLoc) {
    report.psFailed(psLoc);
  }
}
