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
import com.tencent.angel.ml.matrix.transport.PartitionRequest;
import com.tencent.angel.ml.matrix.transport.Response;
import com.tencent.angel.ml.matrix.transport.ResponseType;
import com.tencent.angel.ps.impl.PSContext;
import com.tencent.angel.psagent.matrix.transport.FutureResult;
import io.netty.buffer.ByteBuf;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

/**
 * The asynchronous pusher, it will not wait the push result.
 */
public class AsyncEventPusher extends PS2PSPusherImpl {
  private static final Log LOG = LogFactory.getLog(AsyncEventPusher.class);

  /**
   * The push result checker
   */
  private volatile Thread resultChecker;

  private final AtomicBoolean stopped;

  /**
   * Update request id
   */
  private final AtomicLong idGen;

  /**
   * Update request key to result map
   */
  private final Map<UpdateKey, FutureResult> resultMap;

  /**
   * Create a AsyncPS2PSUpdater
   * @param context PS context
   */
  public AsyncEventPusher(PSContext context) {
    super(context);
    this.stopped = new AtomicBoolean(false);
    this.idGen = new AtomicLong(0);
    this.resultMap = new ConcurrentHashMap<>();
  }

  /**
   * Update request key
   */
  class UpdateKey {
    private final long id;
    private final PartitionKey partKey;
    private final PSLocation psLoc;

    public UpdateKey(long id, PartitionKey partKey, PSLocation psLoc) {
      this.id = id;
      this.partKey = partKey;
      this.psLoc = psLoc;
    }

    @Override public boolean equals(Object o) {
      if (this == o)
        return true;
      if (o == null || getClass() != o.getClass())
        return false;

      UpdateKey updateKey = (UpdateKey) o;

      return id == updateKey.id;
    }

    @Override public int hashCode() {
      return (int) (id ^ (id >>> 32));
    }
  }

  @Override
  public void start() {
    super.start();
    resultChecker = new Thread(()->{
      while (!stopped.get() && !Thread.interrupted()) {
        try {
          Iterator<Map.Entry<UpdateKey, FutureResult>> iter = resultMap.entrySet().iterator();
          while(iter.hasNext()) {
            Map.Entry<UpdateKey, FutureResult> entry = iter.next();
            FutureResult result = entry.getValue();
            if(result.isDone()) {
              Response response = (Response)result.get();
              if(response.getResponseType() != ResponseType.SUCCESS) {
                increaseFailedCounter(entry.getKey().partKey, entry.getKey().psLoc);
              }
              iter.remove();
            }
          }
          Thread.sleep(100);
        } catch (Exception e) {
          if(!stopped.get()) {
            LOG.error("Push result checker interrupted ", e);
          }
        }
      }
    });
    resultChecker.setName("Push-result-checker");
    resultChecker.start();
  }

  @Override
  public void stop() {
    super.stop();
    if (!stopped.getAndSet(true)) {
      if(resultChecker != null) {
        resultChecker.interrupt();
        resultChecker = null;
      }
    }
  }

  @Override public void put(PartitionRequest request, ByteBuf msg, PartitionLocation partLoc) {
    request.setComeFromPs(true);
    msg.resetReaderIndex();
    msg.setBoolean(8, true);

    PartitionKey partKey = request.getPartKey();
    if(partLoc.psLocs.size() == 1) {
      return;
    } else {
      if(partLoc.psLocs.get(0).psId.equals(context.getPSAttemptId().getPsId())) {
        int size = partLoc.psLocs.size();
        for(int i = 1; i < size; i++) {
          resultMap.put(new UpdateKey(idGen.incrementAndGet(), partKey, partLoc.psLocs.get(i)),
            psClient.put(partLoc.psLocs.get(i).psId, partLoc.psLocs.get(i).loc, request, msg.copy()));
        }
        msg.release();
      }
    }
  }
}
