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
package com.tencent.angel.protobuf;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.google.protobuf.ByteString;
import com.google.protobuf.ServiceException;

import com.tencent.angel.PartitionKey;
import com.tencent.angel.common.Location;
import com.tencent.angel.exception.StandbyException;
import com.tencent.angel.master.worker.attempt.WorkerAttempt;
import com.tencent.angel.master.worker.worker.AMWorker;
import com.tencent.angel.master.worker.workergroup.AMWorkerGroup;
import com.tencent.angel.ml.matrix.MatrixContext;
import com.tencent.angel.ml.matrix.MatrixMeta;
import com.tencent.angel.protobuf.generated.MLProtos;
import com.tencent.angel.protobuf.generated.MLProtos.GetMatrixInfoResponse;
import com.tencent.angel.protobuf.generated.MLProtos.LocationProto;
import com.tencent.angel.protobuf.generated.MLProtos.MatrixClock;
import com.tencent.angel.protobuf.generated.MLProtos.Node;
import com.tencent.angel.protobuf.generated.MLProtos.PSAgentAttemptIdProto;
import com.tencent.angel.protobuf.generated.MLProtos.PSAgentIdProto;
import com.tencent.angel.protobuf.generated.MLProtos.PSAttemptIdProto;
import com.tencent.angel.protobuf.generated.MLProtos.PSIdProto;
import com.tencent.angel.protobuf.generated.MLProtos.Pair;
import com.tencent.angel.protobuf.generated.MLProtos.Partition;
import com.tencent.angel.protobuf.generated.MLProtos.TaskIdProto;
import com.tencent.angel.protobuf.generated.MLProtos.WorkerAttemptIdProto;
import com.tencent.angel.protobuf.generated.MLProtos.WorkerGroupIdProto;
import com.tencent.angel.protobuf.generated.MLProtos.WorkerIdProto;
import com.tencent.angel.protobuf.generated.PSAgentMasterServiceProtos.GetAllMatrixInfoResponse;
import com.tencent.angel.protobuf.generated.WorkerMasterServiceProtos.GetWorkerGroupMetaInfoResponse;
import com.tencent.angel.protobuf.generated.WorkerMasterServiceProtos.GetWorkerGroupMetaInfoResponse.WorkerGroupStatus;
import com.tencent.angel.protobuf.generated.WorkerMasterServiceProtos.SplitInfoProto;
import com.tencent.angel.protobuf.generated.WorkerMasterServiceProtos.TaskMetaInfoProto;
import com.tencent.angel.protobuf.generated.WorkerMasterServiceProtos.WorkerGroupMetaInfoProto;
import com.tencent.angel.protobuf.generated.WorkerMasterServiceProtos.WorkerLocationProto;
import com.tencent.angel.protobuf.generated.WorkerMasterServiceProtos.WorkerMetaInfoProto;
import com.tencent.angel.ps.PSAttemptId;
import com.tencent.angel.ps.ParameterServerId;
import com.tencent.angel.psagent.PSAgentAttemptId;
import com.tencent.angel.psagent.PSAgentId;
import com.tencent.angel.split.SplitClassification;
import com.tencent.angel.split.SplitInfo;
import com.tencent.angel.utils.SerdeUtils;
import com.tencent.angel.worker.WorkerAttemptId;
import com.tencent.angel.worker.WorkerGroupId;
import com.tencent.angel.worker.WorkerId;
import com.tencent.angel.worker.task.TaskId;
import com.tencent.angel.master.task.AMTask;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;

import it.unimi.dsi.fastutil.ints.Int2IntOpenHashMap;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Protobufs utility.
 */
public final class ProtobufUtil {

  private final static Log LOG = LogFactory.getLog(ProtobufUtil.class);
  static Map<WorkerAttemptIdProto, WorkerAttemptId> protoToWorkerAttemptIdMap =
      new ConcurrentHashMap<WorkerAttemptIdProto, WorkerAttemptId>();
  static Map<WorkerAttemptId, WorkerAttemptIdProto> workerAttemptIdToProtoMap =
      new ConcurrentHashMap<WorkerAttemptId, WorkerAttemptIdProto>();
  
  static Map<TaskIdProto, TaskId> protoToTaskIdMap =
      new ConcurrentHashMap<TaskIdProto, TaskId>();
  static Map<TaskId, TaskIdProto> taskIdToProtoMap =
      new ConcurrentHashMap<TaskId, TaskIdProto>();
  
  static Map<PSAttemptIdProto, PSAttemptId> protoToPSAttemptIdMap =
      new ConcurrentHashMap<PSAttemptIdProto, PSAttemptId>();
  static Map<PSAttemptId, PSAttemptIdProto> psAttemptIdToProtoMap =
      new ConcurrentHashMap<PSAttemptId, PSAttemptIdProto>();
  
  static Map<PSAgentAttemptIdProto, PSAgentAttemptId> protoToPSAgentAttemptIdMap =
      new ConcurrentHashMap<PSAgentAttemptIdProto, PSAgentAttemptId>();
  static Map<PSAgentAttemptId, PSAgentAttemptIdProto> psAgentAttemptIdToProtoMap =
      new ConcurrentHashMap<PSAgentAttemptId, PSAgentAttemptIdProto>();
  
  public static TaskId convertToId(TaskIdProto idProto) {
    TaskId id = protoToTaskIdMap.get(idProto);
    if(id == null){
      id = new TaskId(idProto.getTaskIndex());
      protoToTaskIdMap.put(idProto, id);
    }
    
    return id;
  }

  public static TaskIdProto convertToIdProto(TaskId id) {
    TaskIdProto idProto = taskIdToProtoMap.get(id);
    if(idProto == null){
      idProto = TaskIdProto.newBuilder().setTaskIndex(id.getIndex()).build();
      taskIdToProtoMap.put(id, idProto);
    }
    
    return idProto;
  }
  
  public static WorkerGroupIdProto convertToIdProto(WorkerGroupId id){
    return WorkerGroupIdProto.newBuilder().setWorkerGroupIndex(id.getIndex()).build();
  }
  
  public static WorkerGroupId convertToId(WorkerGroupIdProto idProto){
    return new WorkerGroupId(idProto.getWorkerGroupIndex());
  }
  
  public static WorkerIdProto convertToIdProto(WorkerId id){
    return WorkerIdProto.newBuilder().setWorkerGroupId(convertToIdProto(id.getWorkerGroupId())).setWorkerIndex(id.getIndex()).build();
  }
  
  public static WorkerId convertToId(WorkerIdProto idProto){
    return new WorkerId(convertToId(idProto.getWorkerGroupId()), idProto.getWorkerIndex());
  }
  
  public static PSIdProto convertToIdProto(ParameterServerId id){
    return PSIdProto.newBuilder().setPsIndex(id.getIndex()).build();
  }
  
  public static ParameterServerId convertToId(PSIdProto idProto){
    return new ParameterServerId(idProto.getPsIndex());
  }
  
  public static PSAttemptIdProto convertToIdProto(PSAttemptId id){
    return PSAttemptIdProto.newBuilder().setPsId(convertToIdProto(id.getParameterServerId())).setAttemptIndex(id.getIndex()).build();
  }
  
  public static PSAttemptId convertToId(PSAttemptIdProto idProto){
    return new PSAttemptId(convertToId(idProto.getPsId()), idProto.getAttemptIndex());
  }
  
  public static WorkerAttemptId convertToId(WorkerAttemptIdProto idProto){
    WorkerAttemptId id = protoToWorkerAttemptIdMap.get(idProto);
    if(id == null){
      id = new WorkerAttemptId(convertToId(idProto.getWorkerId()), idProto.getAttemptIndex());
      protoToWorkerAttemptIdMap.put(idProto, id);
    }
    
    return id;
  }
  
  public static WorkerAttemptIdProto convertToIdProto(WorkerAttemptId id) {
    WorkerAttemptIdProto idProto = workerAttemptIdToProtoMap.get(id);
    if(idProto == null){
      idProto = WorkerAttemptIdProto.newBuilder().setWorkerId(convertToIdProto(id.getWorkerId())).setAttemptIndex(id.getIndex()).build();
      workerAttemptIdToProtoMap.put(id, idProto);
    }
    
    return idProto;
  }
  
  public static PSAgentAttemptIdProto convertToIdProto(PSAgentAttemptId id) {
    PSAgentAttemptIdProto idProto = psAgentAttemptIdToProtoMap.get(id);
    if(idProto == null){
      idProto = PSAgentAttemptIdProto.newBuilder().setPsAgentId(convertToIdProto(id.getPsAgentId())).setAttemptIndex(id.getIndex()).build();
      psAgentAttemptIdToProtoMap.put(id, idProto);
    }
    return idProto;
  }
  
  public static PSAgentIdProto convertToIdProto(PSAgentId id) {
    return PSAgentIdProto.newBuilder().setPsAgentIndex(id.getIndex()).build();
  }

  public static PSAgentAttemptId convertToId(PSAgentAttemptIdProto idProto) {
    PSAgentAttemptId id = protoToPSAgentAttemptIdMap.get(idProto);
    if(id == null){
      id = new PSAgentAttemptId(convertToId(idProto.getPsAgentId()), idProto.getAttemptIndex());
      protoToPSAgentAttemptIdMap.put(idProto, id);
    }
    
    return id;
  }
  
  public static PSAgentId convertToId(PSAgentIdProto id) {
    return new PSAgentId(id.getPsAgentIndex());
  }

  public static IOException getRemoteException(ServiceException se) {
    Throwable e = se.getCause();
    if (e == null) {
      return new IOException(se);
    }
    if (e instanceof StandbyException) {
      return new IOException(e);
    }
    return e instanceof IOException ? (IOException) e : new IOException(se);
  }

  public static Pair buildPair(String key, String value) {
    Pair.Builder builder = Pair.newBuilder();
    builder.setKey(key).setValue(value);
    return builder.build();
  }

  public static PartitionKey convertPartition(Partition partition) {
    return new PartitionKey(partition.getPartitionId(), partition.getMatrixId(),
        partition.getStartRow(), partition.getStartCol(), partition.getEndRow(),
        partition.getEndCol());
  }

  public static Location convertLocation(Node node) {
    return new Location(node.getIp(), node.getPort());
  }

  public static Partition convertPartition(PartitionKey partition) {
    Partition.Builder builder = Partition.newBuilder();
    builder.setEndCol(partition.getEndCol());
    builder.setEndRow(partition.getEndRow());
    builder.setMatrixId(partition.getMatrixId());
    builder.setPartitionId(partition.getPartitionId());
    builder.setStartCol(partition.getStartCol());
    builder.setStartRow(partition.getStartRow());
    return builder.build();
  }

  public static LocationProto convertLocation(Location location) {
    LocationProto.Builder proto = LocationProto.newBuilder();
    proto.setIp(location.getIp());
    proto.setPort(location.getPort());
    return proto.build();
  }

  public static Node buildNode(String string, Location location) {
    Node.Builder nodeBuilder = Node.newBuilder();
    nodeBuilder.setId(string);
    nodeBuilder.setIp(location.getIp());
    nodeBuilder.setPort(location.getPort());
    return nodeBuilder.build();
  }

  private static LocationProto buildLocation(Location location) {
    return LocationProto.newBuilder().setIp(location.getIp()).setPort(location.getPort()).build();
  }

  public static int[] convertUpdateIndexUseKryo(ByteString neededColIndexes) {
    Input input = new Input(neededColIndexes.toByteArray());
    Kryo kryo = new Kryo();
    kryo.register(Integer.class);

    int size = kryo.readObject(input, Integer.class);
    int[] ret = new int[size];
    for (int i = 0; i < size; i++) {
      ret[i] = kryo.readObject(input, Integer.class);
    }
    return ret;
  }

  public static HashMap<PartitionKey, ParameterServerId> getMatrixRoutingInfo(
      GetAllMatrixInfoResponse response) {
    HashMap<PartitionKey, ParameterServerId> partitionPSIndex;
    if (response.getMatrixStatus() == MLProtos.MatrixStatus.M_OK) {
      partitionPSIndex = new HashMap<>();
      for (MLProtos.MatrixProto matrixProto : response.getMatrixInfoList()) {
        for (MLProtos.MatrixPartitionLocation location : matrixProto.getMatrixPartLocationList()) {
          PartitionKey partitionKey = convertPartition(location.getPart());
          partitionPSIndex.put(partitionKey, convertToId(location.getPsId()));
        }
      }
    } else {
      LOG.error("Cannot get matrix routing information from a not ready master");
      return null;
    }
    return partitionPSIndex;
  }

  public static HashMap<PartitionKey, ParameterServerId> getMatrixRoutingInfo(
      GetMatrixInfoResponse response) {
    HashMap<PartitionKey, ParameterServerId> partitionPSIndex = null;
    if (response.getMatrixStatus() == MLProtos.MatrixStatus.M_OK) {
      partitionPSIndex = new HashMap<>();
      for (MLProtos.MatrixPartitionLocation location : response.getMatrixInfo()
          .getMatrixPartLocationList()) {
        PartitionKey partitionKey = ProtobufUtil.convertPartition(location.getPart());
        partitionPSIndex.put(partitionKey, convertToId(location.getPsId()));
      }
    } else {
      LOG.error("Cannot get matrix routing information from a not ready master");
      return null;
    }
    return partitionPSIndex;
  }

  public static HashMap<Integer, MatrixMeta> getMatrixMetaInfo(GetAllMatrixInfoResponse response) {
    HashMap<Integer, MatrixMeta> matrixMetaHashMap = null;

    assert (response.getMatrixStatus() == MLProtos.MatrixStatus.M_OK);
    matrixMetaHashMap = new HashMap<>();
    for (MLProtos.MatrixProto matrixProto : response.getMatrixInfoList()) {
      int matrixId = matrixProto.getId();
      matrixMetaHashMap.put(matrixId, new MatrixMeta(matrixProto));
    }

    return matrixMetaHashMap;
  }

  public static GetWorkerGroupMetaInfoResponse buildGetWorkerGroupMetaResponse(AMWorkerGroup group,
      SplitClassification splits, Configuration conf) throws IOException {
    return GetWorkerGroupMetaInfoResponse.newBuilder()
        .setWorkerGroupStatus(WorkerGroupStatus.WORKERGROUP_OK)
        .setWorkerGroupMeta(buildWorkerGroupMeta(group, splits, conf)).build();
  }

  private static WorkerGroupMetaInfoProto buildWorkerGroupMeta(AMWorkerGroup group,
      SplitClassification splits, Configuration conf) throws IOException {
    WorkerGroupMetaInfoProto.Builder builder = WorkerGroupMetaInfoProto.newBuilder();
    builder.setWorkerGroupId(convertToIdProto(group.getId()));
    builder.setLeaderId(convertToIdProto(group.getLeader()));

    for (AMWorker w : group.getWorkerSet()) {
      builder.addWorkers(buildWorkerMetaProto(w));
    }

    if(splits != null) {
      List<SplitInfo> splitInfoList = SerdeUtils.serilizeSplits(splits, conf);
      SplitInfoProto.Builder splitBuilder = SplitInfoProto.newBuilder();
      for (SplitInfo split : splitInfoList) {
        builder.addSplits(splitBuilder.setSplitClass(split.getSplitClass())
            .setSplit(ByteString.copyFrom(split.getSplit())).build());
      }
    }
    return builder.build();
  }

  private static WorkerMetaInfoProto buildWorkerMetaProto(AMWorker worker) {
    WorkerMetaInfoProto.Builder builder = WorkerMetaInfoProto.newBuilder();
    
    WorkerAttempt attempt = worker.getRunningAttempt();
    WorkerAttemptIdProto workerAttemptIdProto = convertToIdProto(attempt.getId());
    Location location = attempt.getLocation();

    WorkerLocationProto.Builder locBuilder = WorkerLocationProto.newBuilder();
    locBuilder.setWorkerAttemptId(workerAttemptIdProto);
    if (location != null) {
      locBuilder.setLocation(buildLocation(location));
    }
    builder.setWorkerLocation(locBuilder.build());

    TaskMetaInfoProto.Builder taskMetaBuilder = TaskMetaInfoProto.newBuilder();
    MatrixClock.Builder clockBuilder = MatrixClock.newBuilder();
    for (Entry<TaskId, AMTask> taskEntry : attempt.getTaskMap().entrySet()) {
      AMTask task = taskEntry.getValue();
      taskMetaBuilder.setTaskId(convertToIdProto(taskEntry.getKey()));
      taskMetaBuilder.setIteration(task.getIteration());

      Int2IntOpenHashMap matrixClocks = task.getMatrixClocks();
      for (it.unimi.dsi.fastutil.ints.Int2IntMap.Entry clockEntry : matrixClocks
          .int2IntEntrySet()) {
        taskMetaBuilder.addMatrixClock(clockBuilder.setMatrixId(clockEntry.getIntKey())
            .setClock(clockEntry.getIntValue()).build());
      }
      builder.addTasks(taskMetaBuilder.build());
      LOG.debug("task meta=" + taskMetaBuilder.build());
    }

    return builder.build();
  }

  public static SplitClassification getSplitClassification(List<SplitInfoProto> splitProtos,
      Configuration conf) throws ClassNotFoundException, IOException {
    return SerdeUtils.deSerilizeSplitProtos(splitProtos, conf);
  }

  public static MLProtos.MatrixProto generateMatrixProto(MatrixContext mContext, List<MLProtos.Partition> partitions) {
    MLProtos.MatrixProto.Builder mProtoBuilder = MLProtos.MatrixProto.newBuilder();
    mProtoBuilder.setName(mContext.getName());
    mProtoBuilder.setId(mContext.getId());
    mProtoBuilder.setRowNum(mContext.getRowNum());
    mProtoBuilder.setColNum(mContext.getColNum());
    mProtoBuilder.setRowType(mContext.getRowType());
    // set MatrixPartitionLocation
    MLProtos.MatrixPartitionLocation.Builder mpLocBuild = MLProtos.MatrixPartitionLocation.newBuilder();

    for (Partition part : partitions) {
      mpLocBuild.setPart(part);
      mpLocBuild.setPsId(PSIdProto.newBuilder().setPsIndex(mContext.getPartitioner().assignPartToServer(part.getPartitionId())).build());
      mProtoBuilder.addMatrixPartLocation(mpLocBuild.build());
    }
    LOG.info("Matrix " + mProtoBuilder.getName()+ " partition num: " + mProtoBuilder
      .getMatrixPartLocationCount());

    // set attribute
    Pair.Builder attrBuilder = Pair.newBuilder();
    for (Map.Entry<String, String> entry : mContext.getAttributes().entrySet()) {
      attrBuilder.setKey(entry.getKey());
      attrBuilder.setValue(entry.getValue());
      mProtoBuilder.addAttribute(attrBuilder.build());
    }
    return mProtoBuilder.build();
  }
}
