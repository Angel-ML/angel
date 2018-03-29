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

package com.tencent.angel.ml.optimizer.sgd;

import com.tencent.angel.exception.AngelException;
import com.tencent.angel.worker.task.BaseTask;
import com.tencent.angel.worker.task.TaskContext;

import java.io.IOException;

public class DummyTask extends BaseTask<Long, Long, Long> {

  public DummyTask(TaskContext taskContext) throws IOException {
    super(taskContext);
    // TODO Auto-generated constructor stub
  }

  @Override public Long parse(Long key, Long value) {
    return null;
  }

  @Override public void run(TaskContext taskContext) throws AngelException {
    try {
      while (true) {
        Thread.sleep(10000);
      }
    } catch (InterruptedException ie) {

    }

  }

  @Override public void preProcess(TaskContext taskContext) {

  }

}
