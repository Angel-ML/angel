/*
 * Tencent is pleased to support the open source community by making Angel available.
 *
 * Copyright (C) 2017-2018 THL A29 Limited, a Tencent company. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in 
 * compliance with the License. You may obtain a copy of the License at
 *
 * https://opensource.org/licenses/Apache-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 *
 */

package com.tencent.angel.ps.backup.ha.push;

import com.tencent.angel.PartitionKey;
import com.tencent.angel.ml.matrix.PartitionLocation;
import com.tencent.angel.ml.matrix.transport.PartitionRequest;
import com.tencent.angel.ml.matrix.transport.Response;
import com.tencent.angel.ml.matrix.transport.ResponseType;
import com.tencent.angel.ps.impl.PSContext;
import com.tencent.angel.psagent.matrix.transport.FutureResult;
import io.netty.buffer.ByteBuf;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.util.ArrayList;
import java.util.List;

/**
 * The synchronous pusher, it will wait the push result.
 */
public class SyncEventPusher extends PS2PSPusherImpl {
  private static final Log LOG = LogFactory.getLog(SyncEventPusher.class);

  /**
   * Create a SyncPS2PSPusher
   * @param context PS context
   */
  public SyncEventPusher(PSContext context) {
    super(context);
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
        List<FutureResult> results = new ArrayList<>(size - 1);
        for(int i = 1; i < size; i++) {
          results.add(psClient.put(partLoc.psLocs.get(i).psId, partLoc.psLocs.get(i).loc, request, msg.copy()));
        }
        msg.release();

        for(int i = 0; i < size - 1; i++) {
          try {
            Response result = (Response)results.get(i).get();
            if(result.getResponseType() != ResponseType.SUCCESS) {
              increaseFailedCounter(partKey, partLoc.psLocs.get(i + 1));
            }
          } catch (Exception e) {
            LOG.error("wait for result for sync failed ", e);
            increaseFailedCounter(partKey, partLoc.psLocs.get(i + 1));
          }
        }
      }
    }
  }
}
