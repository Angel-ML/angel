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

package com.tencent.angel.webapp.page;

import com.google.inject.Inject;
import com.google.protobuf.ServiceException;
import com.tencent.angel.exception.UnvalidIdStrException;
import com.tencent.angel.master.app.AMContext;
import com.tencent.angel.master.workerclient.WorkerClient;
import com.tencent.angel.worker.WorkerAttemptId;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.yarn.webapp.view.HtmlBlock;

import java.io.IOException;

import static com.tencent.angel.webapp.AngelParams.WORKER_ATTEMPT_ID;
import static org.apache.hadoop.yarn.util.StringHelper.join;

public class WorkerThreadStackBlock extends HtmlBlock {
  final AMContext amContext;
  private static final Log LOG = LogFactory.getLog(ParameterServerThreadStackBlock.class);

  @Inject
  WorkerThreadStackBlock(AMContext amctx) {
    amContext = amctx;
    amContext.getParameterServerManager();
  }

  @Override
  protected void render(Block html) {
    set(TITLE, join("Angel WorkerThreadStack ", $(WORKER_ATTEMPT_ID)));

    try {
      WorkerAttemptId workerAttemptId = new WorkerAttemptId($(WORKER_ATTEMPT_ID));
      WorkerClient workerClient = null;
      LOG.info("start init WorkerClient");
      workerClient = new WorkerClient(amContext, workerAttemptId);
      String info = workerClient.getThreadStack();
      html.pre()._(info)._();
    } catch (IOException | UnvalidIdStrException | ServiceException e) {
      LOG.error("get stack for " + $(WORKER_ATTEMPT_ID) + " failed, ", e);
    }
  }
}
