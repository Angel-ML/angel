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

import com.tencent.angel.conf.AngelConf;
import com.tencent.angel.exception.AngelException;
import com.tencent.angel.worker.storage.*;
import org.apache.hadoop.conf.Configuration;

/**
 * Base for task.
 * <p>
 *   The realization define at implementation and will invoke by {@link Task}
 *   The train data was read fully to {@link DataBlock} at pre-process as default.
 *   <ol>
 *   <li>
 *     Normally communicate with {@link com.tencent.angel.ps.impl.ParameterServer}
 *     do pull or push data(see more detail {@link com.tencent.angel.psagent.matrix.transport.MatrixTransportClient}).
 *   </li>
 *   <li>
 *     The train data read by {@link TaskContext#getReader()}
 *   </li>
 * </ol>
 * </p>
 * @see Task
 * @see com.tencent.angel.worker.Worker
 * @see TaskContext
 * @see com.tencent.angel.psagent.PSAgentContext
 */
public abstract class BaseTask<KEY_IN, VALUE_IN, VALUE_OUT> implements BaseTaskInterface<KEY_IN, VALUE_IN, VALUE_OUT> {
  protected Configuration conf;

  /**
   * The Train data storage.
   */
  protected final DataBlock<VALUE_OUT> taskDataBlock;

  public BaseTask(TaskContext taskContext) {
    String storageLevel =
        taskContext.getConf().get(AngelConf.ANGEL_TASK_DATA_STORAGE_LEVEL,
                AngelConf.DEFAULT_ANGEL_TASK_DATA_STORAGE_LEVEL);

    if (storageLevel.equals("memory")) {
      taskDataBlock = new MemoryDataBlock<VALUE_OUT>(-1);
    } else if (storageLevel.equals("memory_disk")) {
      taskDataBlock = new MemoryAndDiskDataBlock<VALUE_OUT>(taskContext.getTaskId().getIndex());
    } else {
      taskDataBlock = new DiskDataBlock<VALUE_OUT>(taskContext.getTaskId().getIndex());
    }
    conf = taskContext.getConf();


  }


  /**
   * Parse each sample into a labeled data, of which X is the feature weight vector, Y is label.
   */
  @Override
  public abstract VALUE_OUT parse(KEY_IN key, VALUE_IN value);

  /**
   * Preprocess the dataset, parse each data into a labeled data first, and put into training
   * data storage and validation data storage seperately then.
   */

  @Override
  public void preProcess(TaskContext taskContext) {
    try {
      Reader<KEY_IN, VALUE_IN> reader = taskContext.getReader();
      while (reader.nextKeyValue()) {
        VALUE_OUT out = parse(reader.getCurrentKey(), reader.getCurrentValue());
        if (out != null) {
          taskDataBlock.put(out);
        }
      }

      taskDataBlock.flush();
    } catch (Exception e){
      throw new AngelException("Pre-Process Error.", e);
    }
  }

  @Override
  public abstract void run(TaskContext taskContext) throws AngelException;
}
