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

package com.tencent.angel.ps.backup.ha.push;

import com.tencent.angel.PartitionKey;
import com.tencent.angel.ml.matrix.PartitionLocation;
import com.tencent.angel.ml.matrix.transport.PSLocation;
import com.tencent.angel.ml.matrix.transport.Response;
import com.tencent.angel.ml.matrix.transport.ResponseType;
import com.tencent.angel.ps.client.PSClient;
import com.tencent.angel.ps.impl.PSContext;
import com.tencent.angel.ps.impl.matrix.PartitionState;
import com.tencent.angel.ps.impl.matrix.ServerPartition;
import com.tencent.angel.ps.recovery.ha.RecoverPartKey;
import com.tencent.angel.psagent.matrix.transport.FutureResult;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * Base class of backup pusher
 */
public abstract class PS2PSPusherImpl implements PS2PSPusher {
  private static final Log LOG = LogFactory.getLog(PS2PSPusherImpl.class);
  /**
   * PS context
   */
  protected final PSContext context;

  /**
   * PS client
   */
  protected final PSClient psClient;

  /**
   * Push worker pool
   */
  protected volatile ExecutorService workerPool;

  /**
   * Failed push operation statistics for each partition and each ps
   */
  protected final Map<PartitionKey, Map<PSLocation, Integer>> failedUpdateCounters;

  private final ReadWriteLock lock;

  private final AtomicBoolean stopped;

  private volatile Thread recoverChecker;

  /**
   * Create a PS2PSPusherImpl
   * @param context PS context
   */
  public PS2PSPusherImpl(PSContext context) {
    this.context = context;
    this.psClient = new PSClient(context);
    this.failedUpdateCounters = new HashMap<>();
    this.lock = new ReentrantReadWriteLock();
    this.stopped = new AtomicBoolean(false);
  }

  /**
   * Init
   */
  public void init() {
    psClient.init();
  }

  /**
   * Start
   */
  public void start() {
    psClient.start();
    workerPool = Executors.newFixedThreadPool(16);
    recoverChecker = new Thread(() -> {
      while(!stopped.get() && !Thread.interrupted()) {
        try {
          Thread.sleep(30000);
          Map<RecoverPartKey, FutureResult> futures = new HashMap<>();
          try {
            lock.readLock().lock();
            for(Map.Entry<PartitionKey, Map<PSLocation, Integer>> partEntry : failedUpdateCounters.entrySet()) {
              PartitionKey partKey = partEntry.getKey();
              Map<PSLocation, Integer> failedCounters = partEntry.getValue();
              if(failedCounters.isEmpty()) {
                continue;
              }
              PartitionLocation partLoc = context.getMaster().getPartLocation(partKey.getMatrixId(), partKey.getPartitionId());
              if(partLoc.psLocs.size() > 1 && partLoc.psLocs.get(0).psId.equals(context.getPSAttemptId().getPsId())) {
                for(int i = 1; i < partLoc.psLocs.size(); i++) {
                  PSLocation psLoc = partLoc.psLocs.get(i);
                  if(failedCounters.containsKey(psLoc) && failedCounters.get(psLoc) > 0) {
                    RecoverPartKey recoverPartKey = new RecoverPartKey(partKey, psLoc);
                    futures.put(recoverPartKey, recover(recoverPartKey));
                  }
                }
              }
            }
          } finally {
            lock.readLock().unlock();
          }

          waitResults(futures);
        } catch (Throwable e) {
          if(!stopped.get()) {
            LOG.error("Start to ");
          }
        }
      }
    });
    recoverChecker.setName("Recover-checker");
    recoverChecker.start();
  }

  protected void waitResults(Map<RecoverPartKey, FutureResult> futures)
    throws ExecutionException, InterruptedException {
    for(Map.Entry<RecoverPartKey, FutureResult> entry : futures.entrySet()) {
      Response response = (Response)entry.getValue().get();
      if(response.getResponseType() == ResponseType.SUCCESS) {
        clearFailedCounter(entry.getKey().partKey, entry.getKey().psLoc);
      }
      if(response.getResponseType() == ResponseType.NETWORK_ERROR
        || response.getResponseType() == ResponseType.TIMEOUT) {
        context.getPSFailedReport().psFailed(entry.getKey().psLoc);
      }
    }
  }

  /**
   * Stop
   */
  public void stop() {
    if (!stopped.getAndSet(true)) {
      psClient.stop();
      if(recoverChecker != null) {
        recoverChecker.interrupt();
      }
      if(workerPool != null) {
        workerPool.shutdownNow();
      }
    }
  }

  /**
   * Update push operation failed statistics
   * @param partKey partition key
   * @param psLoc ps location
   */
  protected void increaseFailedCounter(PartitionKey partKey, PSLocation psLoc) {
    try {
      lock.writeLock().lock();
      Map<PSLocation, Integer> psToCounters = failedUpdateCounters.get(partKey);
      if(psToCounters == null) {
        psToCounters = new HashMap<>();
        failedUpdateCounters.put(partKey, psToCounters);
      }

      if(psToCounters.containsKey(psLoc)) {
        psToCounters.put(psLoc, psToCounters.get(psLoc) + 1);
      } else {
        psToCounters.put(psLoc, 1);
      }
    } finally {
      lock.writeLock().unlock();
    }
  }

  /**
   * Reset the push operation failed statistics
   * @param partKey partition key
   * @param psLoc ps location
   */
  protected void clearFailedCounter(PartitionKey partKey, PSLocation psLoc) {
    try {
      lock.writeLock().lock();
      Map<PSLocation, Integer> psToCounters = failedUpdateCounters.get(partKey);
      if(psToCounters != null) {
        if(psToCounters.containsKey(psLoc)) {
          psToCounters.put(psLoc, 0);
        }
      }
    } finally {
      lock.writeLock().unlock();
    }
  }

  @Override
  public void updateClock(PartitionKey partKey,  int taskIndex, int clock, PartitionLocation partLoc) {
    if(partLoc.psLocs.size() == 1) {
      return;
    } else {
      if(partLoc.psLocs.get(0).psId.equals(context.getPSAttemptId().getPsId())) {
        int size = partLoc.psLocs.size();
        List<FutureResult> results = new ArrayList<>(size - 1);
        for(int i = 1; i < size; i++) {
          results.add(psClient.updateClock(partLoc.psLocs.get(i).psId, partLoc.psLocs.get(i).loc, partKey, taskIndex, clock));
        }

        size = results.size();
        for(int i = 0; i < size; i++) {
          try {
            Response result = (Response)results.get(i).get();
            if(result.getResponseType() != ResponseType.SUCCESS) {
              if(result.getResponseType() == ResponseType.NETWORK_ERROR
                || result.getResponseType() == ResponseType.TIMEOUT) {
                context.getPSFailedReport().psFailed(partLoc.psLocs.get(i));
              }
              increaseFailedCounter(partKey, partLoc.psLocs.get(i));
            }
          } catch (Exception e) {
            LOG.error("wait for result for sync failed ", e);
            increaseFailedCounter(partKey, partLoc.psLocs.get(i));
          }
        }
      }
    }
  }

  @Override public FutureResult<Response> recover(final RecoverPartKey partKey) {
    FutureResult<Response> result = new FutureResult<>();
    workerPool.execute(()->{
      ServerPartition part = context.getMatrixStorageManager().getPart(partKey.partKey.getMatrixId(), partKey.partKey.getPartitionId());
      if(part == null) {
        result.set(new Response(ResponseType.UNKNOWN_ERROR, "Can not find partition "
          + partKey.partKey.getMatrixId() + ":" + partKey.partKey.getPartitionId()));
        return;
      }

      try {
        part.waitAndSetReadOnly();
        result.set(psClient.recoverPart(partKey.psLoc.psId, partKey.psLoc.loc, part).get());
      } catch (Throwable e) {
        result.set(new Response(ResponseType.NETWORK_ERROR, e.getMessage()));
        LOG.error("handle recover event " + partKey + " failed ", e);
      } finally {
        part.setState(PartitionState.READ_AND_WRITE);
        LOG.info("ps " + context.getPSAttemptId() + " set partition " + part.getPartitionKey() + " to " + part.getState());
      }
    });

    return result;
  }
}
