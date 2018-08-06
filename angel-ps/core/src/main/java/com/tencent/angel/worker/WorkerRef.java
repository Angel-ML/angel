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

package com.tencent.angel.worker;

import com.tencent.angel.common.location.Location;
import com.tencent.angel.protobuf.ProtobufUtil;
import com.tencent.angel.protobuf.generated.MLProtos.LocationProto;
import com.tencent.angel.protobuf.generated.MLProtos.WorkerAttemptIdProto;
import com.tencent.angel.protobuf.generated.WorkerMasterServiceProtos.TaskMetaInfoProto;
import com.tencent.angel.worker.task.TaskContext;
import com.tencent.angel.worker.task.TaskId;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Worker reference,refer {@link Location} and {@link TaskContext}
 */
public class WorkerRef {
  private static final Log LOG = LogFactory.getLog(Worker.class);
  private final WorkerAttemptId workerAttemptId;
  private final Location location;
  private final Map<TaskId, TaskContext> taskIdToContextMap;

  public WorkerRef(WorkerAttemptIdProto workerIdProto, LocationProto location, List<TaskMetaInfoProto> tasks) {
    this.workerAttemptId = ProtobufUtil.convertToId(workerIdProto);
    this.location = new Location(location.getIp(), location.getPort());
    
    int size = tasks.size();
    this.taskIdToContextMap = new HashMap<TaskId, TaskContext>(size);
    
    TaskMetaInfoProto taskMeta = null;
    for (int i = 0; i < size; i++) {
      taskMeta = tasks.get(i);
      LOG.debug("taskMeta=" + taskMeta);
      taskIdToContextMap.put(ProtobufUtil.convertToId(tasks.get(i).getTaskId()), new TaskContext(taskMeta));
    }
  }

  /**
   * Gets location.
   *
   * @return the location
   */
  public Location getLocation() {
    return location;
  }

  /**
   * Gets task num.
   *
   * @return the task num
   */
  public int getTaskNum() {
    return taskIdToContextMap.size();
  }

  /**
   * Gets worker attempt id.
   *
   * @return the worker attempt id
   */
  public WorkerAttemptId getWorkerAttemptId() {
    return workerAttemptId;
  }

  /**
   * Gets tasks context.
   *
   * @return the task id to context map
   */
  public Map<TaskId, TaskContext> getTaskIdToContextMap() {
    return taskIdToContextMap;
  }

}
