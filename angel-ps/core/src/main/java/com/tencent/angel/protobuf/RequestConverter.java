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
package com.tencent.angel.protobuf;

import com.tencent.angel.protobuf.generated.MLProtos.MatrixClock;
import com.tencent.angel.protobuf.generated.MLProtos.Pair;
import com.tencent.angel.protobuf.generated.WorkerMasterServiceProtos.TaskStateProto;
import com.tencent.angel.protobuf.generated.WorkerMasterServiceProtos.WorkerReportRequest;
import com.tencent.angel.psagent.PSAgentContext;
import com.tencent.angel.worker.Worker;
import com.tencent.angel.worker.task.Task;
import com.tencent.angel.worker.task.TaskId;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Helper utility to build protocol buffer requests, or build components for protocol buffer
 * requests.
 */
public final class RequestConverter {

  private static final Log LOG = LogFactory.getLog(RequestConverter.class);

  private RequestConverter() {}


  public static WorkerReportRequest buildWorkerReportRequest(Worker worker) {
    WorkerReportRequest.Builder builder = WorkerReportRequest.newBuilder();
    builder.setWorkerAttemptId(worker.getWorkerAttemptIdProto());
    if (!worker.isWorkerInitFinished()) {
      return builder.build();
    }
    Map<TaskId, Task> tasks = worker.getTaskManager().getRunningTask();
    if (tasks != null && !tasks.isEmpty()) {
      for (Entry<TaskId, Task> entry : tasks.entrySet()) {
        builder.addTaskReports(buildTaskReport(entry.getKey(), entry.getValue()));
      }
    }

    Pair.Builder kvBuilder = Pair.newBuilder();

    Map<String, String> props = worker.getMetrics();
    for (Entry<String, String> kv : props.entrySet()) {
      kvBuilder.setKey(kv.getKey());
      kvBuilder.setValue(kv.getValue());
      builder.addPairs(kvBuilder.build());
    }

    //add the PSAgentContext,need fix
    props = PSAgentContext.get().getMetrics();
    for (Entry<String, String> kv : props.entrySet()) {
      kvBuilder.setKey(kv.getKey());
      kvBuilder.setValue(kv.getValue());
      builder.addPairs(kvBuilder.build());
    }

    return builder.build();
  }


  private static TaskStateProto buildTaskReport(TaskId taskId, Task task) {
    TaskStateProto.Builder builder = TaskStateProto.newBuilder();
    if(!PSAgentContext.get().syncClockEnable()) {
      builder.setIteration(task.getTaskContext().getEpoch());
      Map<Integer, AtomicInteger> matrixClocks = task.getTaskContext().getMatrixClocks();
      MatrixClock.Builder clockBuilder = MatrixClock.newBuilder();
      for (Entry<Integer, AtomicInteger> clockEntry : matrixClocks.entrySet()) {
        builder.addMatrixClocks(clockBuilder.setMatrixId(clockEntry.getKey())
            .setClock(clockEntry.getValue().get()).build());
      }
    }

    builder.setProgress(task.getProgress());
    builder.setState(task.getTaskState().toString());
    builder.setTaskId(ProtobufUtil.convertToIdProto(taskId));

    Pair.Builder kvBuilder = Pair.newBuilder();
    Map<String, AtomicLong> taskCounters = task.getTaskContext().getCounters();
    for (Entry<String, AtomicLong> kv : taskCounters.entrySet()) {
      kvBuilder.setKey(kv.getKey());
      kvBuilder.setValue(String.valueOf(kv.getValue().longValue()));
      builder.addCounters(kvBuilder.build());
    }
    return builder.build();
  }
}
