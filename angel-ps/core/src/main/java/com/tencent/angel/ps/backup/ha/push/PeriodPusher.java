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
import com.tencent.angel.conf.AngelConf;
import com.tencent.angel.ml.matrix.PartitionLocation;
import com.tencent.angel.ml.matrix.transport.PartitionRequest;
import com.tencent.angel.ml.matrix.transport.Response;
import com.tencent.angel.ml.matrix.transport.ResponseType;
import com.tencent.angel.ps.ParameterServerId;
import com.tencent.angel.ps.impl.PSContext;
import com.tencent.angel.ps.impl.matrix.ServerPartition;
import com.tencent.angel.ps.recovery.ha.RecoverPartKey;
import com.tencent.angel.psagent.matrix.transport.FutureResult;
import io.netty.buffer.ByteBuf;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Push the partition to slave pss once in a while
 */
public class PeriodPusher extends PS2PSPusherImpl{
  private static final Log LOG = LogFactory.getLog(PeriodPusher.class);
  /**
   * Push interval in milliseconds
   */
  private final int pushIntervalMs;

  /**
   * Push dispatcher
   */
  private Thread dispatcher;
  private final AtomicBoolean stopped;

  /**
   * Need recover partitions
   */
  private HashMap<PartitionKey, Integer> needRecoverParts;
  private final Lock lock;

  /**
   * Create PeriodPusher
   * @param context PS context
   */
  public PeriodPusher(PSContext context) {
    super(context);
    this.pushIntervalMs = context.getConf().getInt(AngelConf.ANGEL_PS_HA_PUSH_INTERVAL_MS,
      AngelConf.DEFAULT_ANGEL_PS_HA_PUSH_INTERVAL_MS);
    this.stopped = new AtomicBoolean(false);
    this.lock = new ReentrantLock();
    this.needRecoverParts = new HashMap<>();
  }

  /**
   * Init
   */
  public void init() {
    super.init();
  }

  /**
   * Start
   */
  public void start() {
    super.start();
    dispatcher = new Thread(() -> {
      ParameterServerId psId = context.getPSAttemptId().getPsId();
      while(!stopped.get() && !Thread.interrupted()) {
        try {
          Thread.sleep(pushIntervalMs);
          Map<PartitionKey, Integer> parts = getAndClearAllNeedRecoverParts();
          Map<RecoverPartKey, FutureResult> futures = new HashMap<>(parts.size());
          for(PartitionKey part : parts.keySet()) {
            PartitionLocation partLoc = context.getMaster().getPartLocation(part.getMatrixId(), part.getPartitionId());
            if((partLoc.psLocs.size() > 1) && psId.equals(partLoc.psLocs.get(0).psId)) {
              int size = partLoc.psLocs.size();
              for(int i = 1; i < size; i++) {
                RecoverPartKey partKey = new RecoverPartKey(part, partLoc.psLocs.get(i));
                LOG.info("Start to backup partition " + partKey.partKey + " to " + partKey.psLoc);
                futures.put(partKey, recover(partKey));
              }
            }
          }

          waitResults(futures);
        } catch (Exception e) {
          if(!stopped.get()) {
            LOG.error("recover parts failed ", e);
          }
        }
      }
    });

    dispatcher.setName("psha-push-dispatcher");
    dispatcher.start();
  }

  /**
   * Stop
   */
  public void stop() {
    super.stop();
    if (!stopped.getAndSet(true)) {
      if(dispatcher != null) {
        dispatcher.interrupt();
      }
    }
  }

  @Override public void put(PartitionRequest request, ByteBuf msg, PartitionLocation partLoc) {
    increaseUpdateCounter(request.getPartKey());
  }

  @Override
  public FutureResult<Response> recover(RecoverPartKey partKey) {
    FutureResult<Response> result = new FutureResult<>();
    workerPool.execute(()->{
      ServerPartition part = context.getMatrixStorageManager().getPart(partKey.partKey);
      if(part == null) {
        result.set(new Response(ResponseType.UNKNOWN_ERROR, "Can not find partition "
          + partKey.partKey.getMatrixId() + ":" + partKey.partKey.getPartitionId()));
        return;
      }

      try {
        result.set(psClient.recoverPart(partKey.psLoc.psId, partKey.psLoc.loc, part).get());
      } catch (Throwable e) {
        LOG.error("recover part " + partKey + " falied ", e);
        result.set(new Response(ResponseType.UNKNOWN_ERROR, e.getMessage()));
      }
    });

    return result;
  }

  private void increaseUpdateCounter(PartitionKey partKey) {
    try {
      lock.lock();
      if(needRecoverParts.containsKey(partKey)) {
        needRecoverParts.put(partKey, needRecoverParts.get(partKey) + 1);
      } else {
        needRecoverParts.put(partKey, 1);
      }
    } finally {
      lock.unlock();
    }
  }

  private Map<PartitionKey, Integer> getAndClearAllNeedRecoverParts() {
    try {
      lock.lock();
      HashMap<PartitionKey, Integer> ret = needRecoverParts;
      needRecoverParts = new HashMap<>();
      return ret;
    } finally {
      lock.unlock();
    }
  }
}
