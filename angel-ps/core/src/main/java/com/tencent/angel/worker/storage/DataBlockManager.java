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
 */

package com.tencent.angel.worker.storage;

import com.tencent.angel.split.SplitClassification;
import com.tencent.angel.worker.WorkerContext;
import com.tencent.angel.worker.task.TaskId;
import org.apache.hadoop.conf.Configuration;

import java.io.IOException;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * class for data block manager
 */
public class DataBlockManager {

  private boolean useNewAPI;
  private final Map<TaskId, Integer> splitInfos;
  private SplitClassification splitClassification;

  public DataBlockManager() {
    this.splitInfos = new ConcurrentHashMap<TaskId, Integer>();
  }

  public void init() {
    Configuration conf = WorkerContext.get().getConf();
    useNewAPI = conf.getBoolean("mapred.mapper.new-api", false);
  }

  public SplitClassification getSplitClassification() {
    return splitClassification;
  }

  public void setSplitClassification(SplitClassification splitClassification) {
    this.splitClassification = splitClassification;
  }

  /**
   * Assign split to tasks
   *
   * @param set the set
   */
  public void assignSplitToTasks(Set<TaskId> set) {
    if (splitClassification == null) {
      return;
    }

    assert (set.size() == splitClassification.getSplitNum());
    int index = 0;
    for (TaskId id : set) {
      splitInfos.put(id, index);
      index++;
    }
  }

  /**
   * Get the reade for given task
   *
   * @param <K> the type parameter
   * @param <V> the type parameter
   * @param taskId the task id
   * @return the reader
   * @throws IOException the io exception
   * @throws InterruptedException the interrupted exception
   * @throws ClassNotFoundException the class not found exception
   */
  @SuppressWarnings({"rawtypes", "unchecked"})
  public <K, V> Reader<K, V> getReader(TaskId taskId) throws IOException, InterruptedException,
      ClassNotFoundException {
    if (useNewAPI) {
      DFSStorageNewAPI storage =
          new DFSStorageNewAPI(splitClassification.getSplitNewAPI(splitInfos.get(taskId)));
      storage.initReader();
      return storage.getReader();
    } else {
      DFSStorageOldAPI storage =
          new DFSStorageOldAPI(splitClassification.getSplitOldAPI(splitInfos.get(taskId)));
      storage.initReader();
      return storage.getReader();
    }
  }

  public void stop() {
    // TODO Auto-generated method stub

  }
}
