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
import com.tencent.angel.exception.UnvalidIdStrException;
import com.tencent.angel.master.app.AMContext;
import com.tencent.angel.worker.WorkerAttemptId;
import org.apache.hadoop.yarn.webapp.hamlet.Hamlet;
import org.apache.hadoop.yarn.webapp.hamlet.Hamlet.TABLE;
import org.apache.hadoop.yarn.webapp.view.HtmlBlock;

import java.util.Map;

import static com.tencent.angel.webapp.AngelParams.WORKER_ATTEMPT_ID;
import static org.apache.hadoop.yarn.util.StringHelper.join;
import static org.apache.hadoop.yarn.webapp.view.JQueryUI._TH;

public class WorkerCounterBlock extends HtmlBlock {
  final AMContext amContext;

  @Inject
  WorkerCounterBlock(AMContext amctx) {
    amContext = amctx;
  }

  @Override
  protected void render(Block html) {
    set(TITLE, join("Angel WorkerCounterBlock", $(WORKER_ATTEMPT_ID)));

    try {
      WorkerAttemptId workerAttemptId = new WorkerAttemptId($(WORKER_ATTEMPT_ID));

      Map<String, String> metricsMap =
          amContext.getWorkerManager().getWorker(workerAttemptId.getWorkerId())
              .getWorkerAttempt(workerAttemptId).getMetrics();
      TABLE<Hamlet> worker_metrics_table = html.table();
      html.h6($(WORKER_ATTEMPT_ID));

      worker_metrics_table.tr().th(_TH, "NAME").th(_TH, "VALUE")._();
      for (String key : metricsMap.keySet()) {
        String value = metricsMap.get(key);
        worker_metrics_table.tr().td(String.valueOf(key)).td(value)._();
      }
      worker_metrics_table._();
    } catch (UnvalidIdStrException e) {
      LOG.error("unvalid id string, ", e);
    }
  }
}
