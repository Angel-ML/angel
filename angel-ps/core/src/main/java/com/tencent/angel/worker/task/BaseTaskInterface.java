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

package com.tencent.angel.worker.task;

import com.tencent.angel.exception.AngelException;

/**
 * The interface base task.
 *
 * @param <KEYIN>    the key type
 * @param <VALUEIN>  the value type
 * @param <VALUEOUT> the parsed value type
 *
 */
public interface BaseTaskInterface<KEYIN, VALUEIN, VALUEOUT> {
  /**
   * Parse valueout.
   *
   * @param key   the key
   * @param value the value
   * @return the valueout
   */
  VALUEOUT parse(KEYIN key, VALUEIN value);

  /**
   * Pre process.
   *
   * @param taskContext the task context
   */
  void preProcess(TaskContext taskContext);

  /**
   * Run.
   *
   * @param taskContext the task context
   * @throws Exception
   */
  void run(TaskContext taskContext) throws AngelException;
}
