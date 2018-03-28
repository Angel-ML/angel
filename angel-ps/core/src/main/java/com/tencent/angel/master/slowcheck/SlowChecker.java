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

package com.tencent.angel.master.slowcheck;

import com.tencent.angel.common.Id;
import com.tencent.angel.conf.AngelConf;
import com.tencent.angel.master.app.AMContext;
import com.tencent.angel.master.worker.attempt.WorkerAttempt;
import com.tencent.angel.master.worker.attempt.WorkerAttemptEvent;
import com.tencent.angel.master.worker.attempt.WorkerAttemptEventType;
import com.tencent.angel.master.worker.worker.AMWorker;
import com.tencent.angel.worker.WorkerId;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.service.AbstractService;

import java.lang.reflect.Constructor;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Slow workers and pss checker.
 */
public class SlowChecker extends AbstractService {
  private static final Log LOG = LogFactory.getLog(SlowChecker.class);
  private final AMContext context;

  /** check polices*/
  private final List<CheckPolicy> checkPolices;

  /** enable checker or not */
  private final boolean slowCheckEnable;
  private final AtomicBoolean stopped;

  /** check interval in milliseconds*/
  private final int checkIntervalMs;
  private volatile Thread checker;

  /**
   * Construct the service.
   */
  public SlowChecker(AMContext context) {
    super("slow-checker");
    this.context = context;
    checkPolices = new ArrayList<>();
    slowCheckEnable = context.getConf().getBoolean(AngelConf.ANGEL_AM_SLOW_CHECK_ENABLE,
      AngelConf.DEFAULT_ANGEL_AM_SLOW_CHECK_ENABLE);
    checkIntervalMs = context.getConf().getInt(AngelConf.ANGEL_AM_SLOW_CHECK_INTERVAL_MS,
      AngelConf.DEFAULT_ANGEL_AM_SLOW_CHECK_INTERVAL_MS);
    stopped = new AtomicBoolean(false);
  }

  @Override
  protected void serviceStart() throws Exception {
    LOG.info("slowCheckEnable = " + slowCheckEnable + ", checkIntervalMs = " + checkIntervalMs);

    if(slowCheckEnable) {
      checker = new Thread(new Runnable() {
        @Override
        public void run() {
          LOG.info("start slow check thread");
          int size = checkPolices.size();
          while(!stopped.get() && !Thread.interrupted()) {
            for(int i = 0; i < size; i++) {
              List<Id> slowItems = checkPolices.get(i).check(context);
              handleSlowItems(slowItems);
            }

            try {
              Thread.sleep(checkIntervalMs);
            } catch (InterruptedException e) {
              LOG.warn("slow ps/worker checker is interrupted");
            }
          }
        }
      });
      checker.setName("slow-checker");
      checker.start();
    }
  }

  private void handleSlowItems(List<Id> slowItems) {
    if(slowItems != null && slowItems.isEmpty()) {
      int size = slowItems.size();
      for(int i = 0; i < size; i++) {
        handleSlowItem(slowItems.get(i));
      }
    }
  }

  private void handleSlowItem(Id id) {
    LOG.info("slow item " + id + " is checked!!");
    if(id instanceof WorkerId) {
      AMWorker worker = context.getWorkerManager().getWorker((WorkerId) id);

      if(worker.getAttempts().size() < worker.getMaxAttempts()) {
        WorkerAttempt runningAttempt = worker.getRunningAttempt();
        if(runningAttempt != null) {
          context.getEventHandler().handle(new WorkerAttemptEvent(WorkerAttemptEventType.KILL, runningAttempt.getId()));
        }
      }
    }
  }

  @Override
  protected void serviceInit(Configuration conf) throws Exception {
    super.serviceInit(conf);
    if(slowCheckEnable) {
      String polices = conf.get(AngelConf.ANGEL_AM_SLOW_CHECK_POLICES,
        AngelConf.DEFAULT_ANGEL_AM_SLOW_CHECK_POLICES);

      LOG.info("slow check policy list = " + polices);
      String [] policyNames = polices.split(",");
      for(int i = 0; i < policyNames.length; i++) {
        Class<? extends CheckPolicy> policyClass =
          (Class<? extends CheckPolicy>) Class.forName(policyNames[i]);
        Constructor<? extends CheckPolicy> constructor = policyClass.getConstructor();
        constructor.setAccessible(true);
        checkPolices.add(constructor.newInstance());
      }
    }
  }

  @Override
  protected void serviceStop() throws Exception {
    if (stopped.getAndSet(true)) {
      return;
    }
    if (checker != null) {
      checker.interrupt();
      try {
        checker.join();
      } catch (InterruptedException ie) {
        LOG.warn("slow-checker interrupted while stopping");
      }
    }
  }
}
