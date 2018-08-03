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

package com.tencent.angel.webapp.page;

import com.google.inject.Inject;
import com.tencent.angel.exception.UnvalidIdStrException;
import com.tencent.angel.master.app.AMContext;
import com.tencent.angel.worker.task.TaskId;
import org.apache.hadoop.yarn.webapp.view.HtmlBlock;

import static com.tencent.angel.webapp.AngelParams.TASK_ID;
import static org.apache.hadoop.yarn.util.StringHelper.join;

public class TaskCountersBlock extends HtmlBlock {
  final AMContext amContext;

  @Inject
  TaskCountersBlock(AMContext amctx) {
    amContext = amctx;
  }

  @Override
  protected void render(Block html) {
    set(TITLE, join("Angel TaskCountersBlock", $(TASK_ID)));
    String taskIdSr = $(TASK_ID);
    html.h1(taskIdSr);

    try {
      TaskId taskId = new TaskId(taskIdSr);
      String taskCounters =
          amContext.getTaskManager().getTask(taskId).getMetrics().get("taskCounters");
      html.pre()._(taskCounters)._();
    } catch (UnvalidIdStrException e) {
      LOG.error("unvalid id string ", e);
    }
  }
}
