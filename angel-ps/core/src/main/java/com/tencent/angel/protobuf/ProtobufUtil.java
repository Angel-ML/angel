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

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.google.protobuf.ByteString;
import com.google.protobuf.ServiceException;
import com.tencent.angel.PartitionKey;
import com.tencent.angel.common.location.Location;
import com.tencent.angel.exception.StandbyException;
import com.tencent.angel.master.matrix.committer.SaveResult;
import com.tencent.angel.master.task.AMTask;
import com.tencent.angel.master.worker.attempt.WorkerAttempt;
import com.tencent.angel.master.worker.worker.AMWorker;
import com.tencent.angel.master.worker.workergroup.AMWorkerGroup;
import com.tencent.angel.ml.matrix.MatrixContext;
import com.tencent.angel.ml.matrix.MatrixMeta;
import com.tencent.angel.ml.matrix.MatrixReport;
import com.tencent.angel.ml.matrix.PartContext;
import com.tencent.angel.ml.matrix.PartReport;
import com.tencent.angel.ml.matrix.PartitionMeta;
import com.tencent.angel.ml.matrix.RowType;
import com.tencent.angel.model.LoadState;
import com.tencent.angel.model.MatrixLoadContext;
import com.tencent.angel.model.MatrixSaveContext;
import com.tencent.angel.model.ModelLoadContext;
import com.tencent.angel.model.ModelSaveContext;
import com.tencent.angel.model.PSMatricesLoadContext;
import com.tencent.angel.model.PSMatricesLoadResult;
import com.tencent.angel.model.PSMatricesSaveContext;
import com.tencent.angel.model.PSMatricesSaveResult;
import com.tencent.angel.model.PSMatrixLoadContext;
import com.tencent.angel.model.PSMatrixSaveContext;
import com.tencent.angel.model.SaveState;
import com.tencent.angel.protobuf.generated.ClientMasterServiceProtos.MatrixLoadContextProto;
import com.tencent.angel.protobuf.generated.ClientMasterServiceProtos.MatrixSaveContextProto;
import com.tencent.angel.protobuf.generated.ClientMasterServiceProtos.ModelLoadContextProto;
import com.tencent.angel.protobuf.generated.ClientMasterServiceProtos.ModelSaveContextProto;
import com.tencent.angel.protobuf.generated.MLProtos.CreateMatricesRequest;
import com.tencent.angel.protobuf.generated.MLProtos.LocationProto;
import com.tencent.angel.protobuf.generated.MLProtos.MatrixClock;
import com.tencent.angel.protobuf.generated.MLProtos.MatrixContextProto;
import com.tencent.angel.protobuf.generated.MLProtos.MatrixMetaProto;
import com.tencent.angel.protobuf.generated.MLProtos.PSAgentAttemptIdProto;
import com.tencent.angel.protobuf.generated.MLProtos.PSAgentIdProto;
import com.tencent.angel.protobuf.generated.MLProtos.PSAttemptIdProto;
import com.tencent.angel.protobuf.generated.MLProtos.PSFailedReportProto;
import com.tencent.angel.protobuf.generated.MLProtos.PSFailedReportsProto;
import com.tencent.angel.protobuf.generated.MLProtos.PSIdProto;
import com.tencent.angel.protobuf.generated.MLProtos.PSLocationProto;
import com.tencent.angel.protobuf.generated.MLProtos.PSStatus;
import com.tencent.angel.protobuf.generated.MLProtos.Pair;
import com.tencent.angel.protobuf.generated.MLProtos.PartContextProto;
import com.tencent.angel.protobuf.generated.MLProtos.PartitionMetaProto;
import com.tencent.angel.protobuf.generated.MLProtos.TaskIdProto;
import com.tencent.angel.protobuf.generated.MLProtos.WorkerAttemptIdProto;
import com.tencent.angel.protobuf.generated.MLProtos.WorkerGroupIdProto;
import com.tencent.angel.protobuf.generated.MLProtos.WorkerIdProto;
import com.tencent.angel.protobuf.generated.PSMasterServiceProtos.MatrixReportProto;
import com.tencent.angel.protobuf.generated.PSMasterServiceProtos.PSMatricesLoadContextProto;
import com.tencent.angel.protobuf.generated.PSMasterServiceProtos.PSMatricesLoadResultProto;
import com.tencent.angel.protobuf.generated.PSMasterServiceProtos.PSMatricesSaveContextProto;
import com.tencent.angel.protobuf.generated.PSMasterServiceProtos.PSMatricesSaveResultProto;
import com.tencent.angel.protobuf.generated.PSMasterServiceProtos.PSMatrixLoadContextProto;
import com.tencent.angel.protobuf.generated.PSMasterServiceProtos.PSMatrixSaveContextProto;
import com.tencent.angel.protobuf.generated.PSMasterServiceProtos.PartReportProto;
import com.tencent.angel.protobuf.generated.PSMasterServiceProtos.RecoverPartKeyProto;
import com.tencent.angel.protobuf.generated.PSMasterServiceProtos.SaveResultProto;
import com.tencent.angel.protobuf.generated.WorkerMasterServiceProtos.GetWorkerGroupMetaInfoResponse;
import com.tencent.angel.protobuf.generated.WorkerMasterServiceProtos.GetWorkerGroupMetaInfoResponse.WorkerGroupStatus;
import com.tencent.angel.protobuf.generated.WorkerMasterServiceProtos.SplitInfoProto;
import com.tencent.angel.protobuf.generated.WorkerMasterServiceProtos.TaskMetaInfoProto;
import com.tencent.angel.protobuf.generated.WorkerMasterServiceProtos.WorkerGroupMetaInfoProto;
import com.tencent.angel.protobuf.generated.WorkerMasterServiceProtos.WorkerLocationProto;
import com.tencent.angel.protobuf.generated.WorkerMasterServiceProtos.WorkerMetaInfoProto;
import com.tencent.angel.ps.PSAttemptId;
import com.tencent.angel.ps.ParameterServerId;
import com.tencent.angel.ps.ha.RecoverPartKey;
import com.tencent.angel.ps.server.data.PSLocation;
import com.tencent.angel.ps.storage.matrix.PartitionState;
import com.tencent.angel.ps.storage.partitioner.Partitioner;
import com.tencent.angel.psagent.PSAgentAttemptId;
import com.tencent.angel.psagent.PSAgentId;
import com.tencent.angel.split.SplitClassification;
import com.tencent.angel.split.SplitInfo;
import com.tencent.angel.utils.SerdeUtils;
import com.tencent.angel.worker.WorkerAttemptId;
import com.tencent.angel.worker.WorkerGroupId;
import com.tencent.angel.worker.WorkerId;
import com.tencent.angel.worker.task.TaskId;
import it.unimi.dsi.fastutil.ints.Int2IntOpenHashMap;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;

/**
 * Protobufs utility.
 */
public final class ProtobufUtil {

  private final static Log LOG = LogFactory.getLog(ProtobufUtil.class);
  static Map<WorkerAttemptIdProto, WorkerAttemptId> protoToWorkerAttemptIdMap =
      new ConcurrentHashMap<WorkerAttemptIdProto, WorkerAttemptId>();
  static Map<WorkerAttemptId, WorkerAttemptIdProto> workerAttemptIdToProtoMap =
      new ConcurrentHashMap<WorkerAttemptId, WorkerAttemptIdProto>();

  static Map<TaskIdProto, TaskId> protoToTaskIdMap = new ConcurrentHashMap<TaskIdProto, TaskId>();
  static Map<TaskId, TaskIdProto> taskIdToProtoMap = new ConcurrentHashMap<TaskId, TaskIdProto>();

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
    if (id == null) {
      id = new TaskId(idProto.getTaskIndex());
      protoToTaskIdMap.put(idProto, id);
    }

    return id;
  }

  public static TaskIdProto convertToIdProto(TaskId id) {
    TaskIdProto idProto = taskIdToProtoMap.get(id);
    if (idProto == null) {
      idProto = TaskIdProto.newBuilder().setTaskIndex(id.getIndex()).build();
      taskIdToProtoMap.put(id, idProto);
    }

    return idProto;
  }

  public static WorkerGroupIdProto convertToIdProto(WorkerGroupId id) {
    return WorkerGroupIdProto.newBuilder().setWorkerGroupIndex(id.getIndex()).build();
  }

  public static WorkerGroupId convertToId(WorkerGroupIdProto idProto) {
    return new WorkerGroupId(idProto.getWorkerGroupIndex());
  }

  public static WorkerIdProto convertToIdProto(WorkerId id) {
    return WorkerIdProto.newBuilder().setWorkerGroupId(convertToIdProto(id.getWorkerGroupId()))
        .setWorkerIndex(id.getIndex()).build();
  }

  public static WorkerId convertToId(WorkerIdProto idProto) {
    return new WorkerId(convertToId(idProto.getWorkerGroupId()), idProto.getWorkerIndex());
  }

  public static PSIdProto convertToIdProto(ParameterServerId id) {
    return PSIdProto.newBuilder().setPsIndex(id.getIndex()).build();
  }

  public static ParameterServerId convertToId(PSIdProto idProto) {
    return new ParameterServerId(idProto.getPsIndex());
  }

  public static PSAttemptIdProto convertToIdProto(PSAttemptId id) {
    return PSAttemptIdProto.newBuilder().setPsId(convertToIdProto(id.getPsId()))
        .setAttemptIndex(id.getIndex()).build();
  }

  public static PSAttemptId convertToId(PSAttemptIdProto idProto) {
    return new PSAttemptId(convertToId(idProto.getPsId()), idProto.getAttemptIndex());
  }

  public static WorkerAttemptId convertToId(WorkerAttemptIdProto idProto) {
    WorkerAttemptId id = protoToWorkerAttemptIdMap.get(idProto);
    if (id == null) {
      id = new WorkerAttemptId(convertToId(idProto.getWorkerId()), idProto.getAttemptIndex());
      protoToWorkerAttemptIdMap.put(idProto, id);
    }

    return id;
  }

  public static WorkerAttemptIdProto convertToIdProto(WorkerAttemptId id) {
    WorkerAttemptIdProto idProto = workerAttemptIdToProtoMap.get(id);
    if (idProto == null) {
      idProto = WorkerAttemptIdProto.newBuilder().setWorkerId(convertToIdProto(id.getWorkerId()))
          .setAttemptIndex(id.getIndex()).build();
      workerAttemptIdToProtoMap.put(id, idProto);
    }

    return idProto;
  }

  public static PSAgentAttemptIdProto convertToIdProto(PSAgentAttemptId id) {
    PSAgentAttemptIdProto idProto = psAgentAttemptIdToProtoMap.get(id);
    if (idProto == null) {
      idProto = PSAgentAttemptIdProto.newBuilder().setPsAgentId(convertToIdProto(id.getPsAgentId()))
          .setAttemptIndex(id.getIndex()).build();
      psAgentAttemptIdToProtoMap.put(id, idProto);
    }
    return idProto;
  }

  public static PSAgentIdProto convertToIdProto(PSAgentId id) {
    return PSAgentIdProto.newBuilder().setPsAgentIndex(id.getIndex()).build();
  }

  public static PSAgentAttemptId convertToId(PSAgentAttemptIdProto idProto) {
    PSAgentAttemptId id = protoToPSAgentAttemptIdMap.get(idProto);
    if (id == null) {
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


  public static LocationProto convertLocation(Location location) {
    LocationProto.Builder proto = LocationProto.newBuilder();
    proto.setIp(location.getIp());
    proto.setPort(location.getPort());
    return proto.build();
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

    if (splits != null) {
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
        taskMetaBuilder.addMatrixClock(
            clockBuilder.setMatrixId(clockEntry.getIntKey()).setClock(clockEntry.getIntValue())
                .build());
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

  public static CreateMatricesRequest buildCreateMatricesRequest(List<MatrixContext> matrices) {
    CreateMatricesRequest.Builder createMatricesReqBuilder = CreateMatricesRequest.newBuilder();
    if (matrices != null) {
      int size = matrices.size();
      for (int i = 0; i < size; i++) {
        createMatricesReqBuilder.addMatrices(convertToMatrixContextProto(matrices.get(i)));
      }
    }

    return createMatricesReqBuilder.build();
  }

  public static MatrixContextProto convertToMatrixContextProto(MatrixContext mContext) {
    MatrixContextProto.Builder builder =  MatrixContextProto.newBuilder();
    builder.setName(mContext.getName()).setId(mContext.getMatrixId())
        .setRowNum(mContext.getRowNum()).setColNum(mContext.getColNum())
        .setIndexStart(mContext.getIndexStart())
        .setIndexEnd(mContext.getIndexEnd())
        .setValidIndexNum(mContext.getValidIndexNum())
        .setBlockRowNum(mContext.getMaxRowNumInBlock())
        .setBlockColNum(mContext.getMaxColNumInBlock())
        .setRowType(mContext.getRowType().getNumber())
        .setPartitionerClassName(mContext.getPartitionerClass().getName())
        .addAllParts(convertToPartContextsProto(mContext.getParts()))
        .addAllAttribute(convertToPairs(mContext.getAttributes()))
        .setPartitionNum(mContext.getPartitionNum());
    if(mContext.getInitFunc() != null) {
      builder.setInitFunc(ByteString.copyFrom(SerdeUtils.serializeInitFunc(mContext.getInitFunc())));
    }

    return builder.build();
  }

  public static List<PartContext> convertToPartContexts(List<PartContextProto> partsProto) {
    if (partsProto != null && !partsProto.isEmpty()) {
      List<PartContext> parts = new ArrayList<>(partsProto.size());
      for (PartContextProto partProto : partsProto) {
        parts.add(convert(partProto));
      }
      return parts;
    } else {
      return new ArrayList<>();
    }
  }

  public static List<PartContextProto> convertToPartContextsProto(List<PartContext> parts) {
    if (parts != null && !parts.isEmpty()) {
      List<PartContextProto> partsProto = new ArrayList<>(parts.size());
      for (PartContext part : parts) {
        partsProto.add(convert(part));
      }
      return partsProto;
    } else {
      return new ArrayList<>();
    }
  }

  public static PartContext convert(PartContextProto partProto) {
    return new PartContext(partProto.getStartRow(), partProto.getEndRow(), partProto.getStartCol(),
        partProto.getEndCol(), partProto.getIndexNum());
  }

  public static PartContextProto convert(PartContext part) {
    return PartContextProto.newBuilder().setStartRow(part.getStartRow()).setEndRow(part.getEndRow())
        .setStartCol(part.getStartCol()).setEndCol(part.getEndCol()).setIndexNum(part.getIndexNum())
        .build();
  }

  public static List<Pair> convertToPairs(Map<String, String> attrs) {
    Pair.Builder builder = Pair.newBuilder();
    List<Pair> pairs = new ArrayList<>(attrs.size());
    for (Entry<String, String> kv : attrs.entrySet()) {
      pairs.add(builder.setKey(kv.getKey()).setValue(kv.getValue()).build());
    }
    return pairs;
  }

  public static Map<String, String> convertToAttributes(List<Pair> pairs) {
    int size = pairs.size();
    Map<String, String> attrs = new HashMap<>(size);

    for (int i = 0; i < size; i++) {
      attrs.put(pairs.get(i).getKey(), pairs.get(i).getValue());
    }
    return attrs;
  }

  public static List<MatrixContext> convertToMatrixContexts(
      List<MatrixContextProto> matrixContextProtoList) throws ClassNotFoundException {
    List<MatrixContext> matrixContexts = new ArrayList<>(matrixContextProtoList.size());
    int size = matrixContextProtoList.size();
    for (int i = 0; i < size; i++) {
      matrixContexts.add(convertToMatrixContext(matrixContextProtoList.get(i)));
    }
    return matrixContexts;
  }

  public static MatrixContext convertToMatrixContext(MatrixContextProto matrixContextProto)
      throws ClassNotFoundException {
    MatrixContext matrixContext =
        new MatrixContext(matrixContextProto.getName(), matrixContextProto.getRowNum(),
            matrixContextProto.getColNum(), matrixContextProto.getIndexStart(),
            matrixContextProto.getIndexEnd(), matrixContextProto.getValidIndexNum(),
            matrixContextProto.getBlockRowNum(), matrixContextProto.getBlockColNum(),
            convertToPartContexts(matrixContextProto.getPartsList()),
            RowType.valueOf(matrixContextProto.getRowType()));

    matrixContext.setPartitionNum(matrixContextProto.getPartitionNum());
    matrixContext.setMatrixId(matrixContextProto.getId());
    matrixContext.setPartitionerClass(
        (Class<? extends Partitioner>) Class.forName(matrixContextProto.getPartitionerClassName()));
    matrixContext.getAttributes()
        .putAll(convertToAttributes(matrixContextProto.getAttributeList()));
    if(matrixContextProto.hasInitFunc()) {
      matrixContext.setInitFunc(SerdeUtils.deserializeInitFunc(matrixContextProto.getInitFunc().toByteArray()));
    }

    return matrixContext;
  }

  public static MatrixMetaProto convertToMatrixMetaProto(MatrixMeta matrixMeta) {
    MatrixMetaProto.Builder builder = MatrixMetaProto.newBuilder();
    builder.setTotalPartNum(matrixMeta.getTotalPartNum());
    builder.setMatrixContext(convertToMatrixContextProto(matrixMeta.getMatrixContext()));
    Map<Integer, PartitionMeta> matrixMetas = matrixMeta.getPartitionMetas();

    for (Entry<Integer, PartitionMeta> entry : matrixMetas.entrySet()) {
      builder.addPartMetas(convertToParitionMetaProto(entry.getValue()));
    }
    return builder.build();
  }

  public static List<PSIdProto> convertToPSIdProtos(List<ParameterServerId> psIds) {
    int size = psIds.size();
    List<PSIdProto> psIdProtos = new ArrayList<>(size);
    for (int i = 0; i < size; i++) {
      psIdProtos.add(convertToIdProto(psIds.get(i)));
    }
    return psIdProtos;
  }

  public static List<ParameterServerId> convertToPSIds(List<PSIdProto> psIdProtos) {
    int size = psIdProtos.size();
    List<ParameterServerId> psIds = new ArrayList<>(size);
    for (int i = 0; i < size; i++) {
      psIds.add(convertToId(psIdProtos.get(i)));
    }
    return psIds;
  }

  public static PartitionMetaProto convertToParitionMetaProto(PartitionMeta partitionMeta) {
    PartitionMetaProto.Builder builder = PartitionMetaProto.newBuilder();
    builder.setPartitionId(partitionMeta.getPartId()).setStartRow(partitionMeta.getStartRow())
        .setEndRow(partitionMeta.getEndRow()).setStartCol(partitionMeta.getStartCol())
        .setEndCol(partitionMeta.getEndCol())
        .setIndexNum(partitionMeta.getIndexNum())
        .addAllStoredPs(convertToPSIdProtos(partitionMeta.getPss()));

    return builder.build();
  }

  public static PSLocationProto convertToPSLocProto(ParameterServerId psId, Location location) {
    if (location == null) {
      return PSLocationProto.newBuilder().setPsId(convertToIdProto(psId))
          .setPsStatus(PSStatus.PS_NOTREADY).build();
    } else {
      return PSLocationProto.newBuilder().setPsId(convertToIdProto(psId))
          .setLocation(convertToLocationProto(location)).setPsStatus(PSStatus.PS_OK).build();
    }
  }

  public static LocationProto convertToLocationProto(Location location) {
    return LocationProto.newBuilder().setIp(location.getIp()).setPort(location.getPort()).build();
  }

  public static MatrixMeta convertToMatrixMeta(MatrixMetaProto matrixMetaProto)
      throws ClassNotFoundException {
    MatrixContext matrixContext = convertToMatrixContext(matrixMetaProto.getMatrixContext());

    List<PartitionMetaProto> partMetaProtos = matrixMetaProto.getPartMetasList();
    int size = partMetaProtos.size();
    Map<Integer, PartitionMeta> partitionMetas = new HashMap<>(size);
    for (int i = 0; i < size; i++) {
      partitionMetas.put(partMetaProtos.get(i).getPartitionId(),
          convertToParitionMeta(matrixContext.getMatrixId(), partMetaProtos.get(i)));
    }
    return new MatrixMeta(matrixMetaProto.getTotalPartNum(), matrixContext, partitionMetas);
  }

  public static List<MatrixMeta> convertToMatricesMeta(List<MatrixMetaProto> matricesMetaProto)
      throws ClassNotFoundException, IOException {
    int size = matricesMetaProto.size();
    List<MatrixMeta> matricesMeta = new ArrayList<>(size);
    for (int i = 0; i < size; i++) {
      matricesMeta.add(convertToMatrixMeta(matricesMetaProto.get(i)));
    }
    return matricesMeta;
  }

  public static PartitionMeta convertToParitionMeta(int matrixId,
      PartitionMetaProto partMetaProto) {
    return new PartitionMeta(matrixId, partMetaProto.getPartitionId(), partMetaProto.getStartRow(),
        partMetaProto.getEndRow(), partMetaProto.getStartCol(), partMetaProto.getEndCol(),
        partMetaProto.getIndexNum(),
        convertToPSIds(partMetaProto.getStoredPsList()));
  }

  public static Location convertToLocation(PSLocationProto psLocation) {
    if (psLocation.getPsStatus() == PSStatus.PS_NOTREADY) {
      return null;
    }
    return new Location(psLocation.getLocation().getIp(), psLocation.getLocation().getPort());
  }

  public static Location convertToLocation(LocationProto loc) {
    return new Location(loc.getIp(), loc.getPort());
  }

  public static MatrixMetaProto loadMatrixMetaProto(FSDataInputStream input) throws IOException {
    return MatrixMetaProto.parseDelimitedFrom(input);
  }

  public static List<MatrixReport> convertToMatrixReports(List<MatrixReportProto> reportProtos) {
    int size = reportProtos.size();
    List<MatrixReport> reports = new ArrayList<>(size);
    for (int i = 0; i < size; i++) {
      reports.add(convertToMatrixReport(reportProtos.get(i)));
    }

    return reports;
  }

  public static MatrixReport convertToMatrixReport(MatrixReportProto reportProto) {
    return new MatrixReport(reportProto.getMatrixId(),
        convertToPartReports(reportProto.getPartReportsList()));
  }

  public static List<PartReport> convertToPartReports(List<PartReportProto> reportProtos) {
    int size = reportProtos.size();
    List<PartReport> reports = new ArrayList<>(size);
    for (int i = 0; i < size; i++) {
      reports.add(convertToPartReport(reportProtos.get(i)));
    }
    return reports;
  }

  public static PartReport convertToPartReport(PartReportProto reportProto) {
    return new PartReport(reportProto.getPartId(), PartitionState.valueOf(reportProto.getStatus()));
  }

  public static RecoverPartKeyProto convert(RecoverPartKey recoverPartKey) {
    return RecoverPartKeyProto.newBuilder().setMatrixId(recoverPartKey.partKey.getMatrixId())
        .setPartId(recoverPartKey.partKey.getPartitionId())
        .setPsId(convertToIdProto(recoverPartKey.psLoc.psId))
        .setLoc(convertToLocationProto(recoverPartKey.psLoc.loc)).build();
  }

  public static RecoverPartKey convert(RecoverPartKeyProto proto) {
    return new RecoverPartKey(new PartitionKey(proto.getMatrixId(), proto.getPartId()),
        new PSLocation(convertToId(proto.getPsId()), convert(proto.getLoc())));
  }

  public static Location convert(LocationProto locProto) {
    return new Location(locProto.getIp(), locProto.getPort());
  }

  public static List<PSMatrixSaveContext> convertToSaveMatricesContext(
      List<PSMatrixSaveContextProto> protos) {
    List<PSMatrixSaveContext> saveMatrices = new ArrayList<>(protos.size());
    for (PSMatrixSaveContextProto matrixProto : protos) {
      saveMatrices.add(
          new PSMatrixSaveContext(matrixProto.getMatrixId(), matrixProto.getPartIdsList(),
              matrixProto.getRowIndexesList(), matrixProto.getFormatClassName(),
              matrixProto.getSavePath(), matrixProto.getCloneFirst(), matrixProto.getSortFirst()));
    }
    return saveMatrices;
  }

  public static List<PSMatrixSaveContextProto> convertToSaveMatricesContextProto(
      List<PSMatrixSaveContext> contexts) {
    List<PSMatrixSaveContextProto> saveMatrices = new ArrayList<>(contexts.size());
    for (PSMatrixSaveContext context : contexts) {
      saveMatrices.add(PSMatrixSaveContextProto.newBuilder().
          setMatrixId(context.getMatrixId()).addAllPartIds(context.getPartIds())
          .addAllRowIndexes(context.getRowIndexes()).build());
    }
    return saveMatrices;
  }

  public static PSMatrixSaveContext convert(PSMatrixSaveContextProto proto) {
    return new PSMatrixSaveContext(proto.getMatrixId(), proto.getPartIdsList(),
        proto.getRowIndexesList(), proto.getFormatClassName(), proto.getSavePath(),
        proto.getCloneFirst(), proto.getSortFirst());
  }

  public static PSMatrixSaveContextProto convert(PSMatrixSaveContext context) {
    return PSMatrixSaveContextProto.newBuilder().
        setMatrixId(context.getMatrixId()).addAllPartIds(context.getPartIds())
        .setFormatClassName(context.getFormatClassName()).setSavePath(context.getSavePath())
        .setCloneFirst(context.cloneFirst()).setSortFirst(context.sortFirst())
        .addAllRowIndexes(context.getRowIndexes()).build();
  }

  public static PSFailedReportsProto convertToPSFailedReportsProto(
      HashMap<PSLocation, Integer> reports) {
    PSFailedReportsProto.Builder builder = PSFailedReportsProto.newBuilder();
    for (Entry<PSLocation, Integer> entry : reports.entrySet()) {
      builder.addPsFailedReports(convert(entry.getKey(), entry.getValue()));
    }
    return builder.build();
  }

  public static PSFailedReportProto convert(PSLocation psLoc, int counter) {
    return PSFailedReportProto.newBuilder().setPsLoc(convert(psLoc)).setFailedCounter(counter)
        .build();
  }

  public static PSLocationProto convert(PSLocation psLoc) {
    return PSLocationProto.newBuilder().setPsId(convertToIdProto(psLoc.psId))
        .setLocation(convertToLocationProto(psLoc.loc)).setPsStatus(PSStatus.PS_OK).build();
  }


  public static HashMap<PSLocation, Integer> convert(PSFailedReportsProto reportsProto) {
    HashMap<PSLocation, Integer> reports = new HashMap<>();
    List<PSFailedReportProto> reportList = reportsProto.getPsFailedReportsList();
    int size = reportList.size();
    for (int i = 0; i < size; i++) {
      reports.put(new PSLocation(convertToId(reportList.get(i).getPsLoc().getPsId()),
              convert(reportList.get(i).getPsLoc().getLocation())),
          reportList.get(i).getFailedCounter());
    }
    return reports;
  }

  public static PSLocation convert(PSLocationProto psLoc) {
    return new PSLocation(convertToId(psLoc.getPsId()), convertToLocation(psLoc.getLocation()));
  }

  public static ModelSaveContextProto convert(ModelSaveContext saveContext) {
    List<MatrixSaveContext> matricesContext = saveContext.getMatricesContext();
    ModelSaveContextProto.Builder builder = ModelSaveContextProto.newBuilder();
    builder.setSavePath(saveContext.getSavePath());
    builder.setCheckpint(saveContext.isCheckpoint());
    int size = matricesContext.size();
    for (int i = 0; i < size; i++) {
      builder.addMatrixContextes(convert(matricesContext.get(i)));
    }

    return builder.build();
  }

  public static MatrixSaveContextProto convert(MatrixSaveContext saveContext) {
    return MatrixSaveContextProto.newBuilder().setMatrixName(saveContext.getMatrixName())
        .setFormatClassName(saveContext.getFormatClassName())
        .addAllRowIndexes(saveContext.getRowIndexes()).build();
  }

  public static ModelSaveContext convert(ModelSaveContextProto saveContext) {
    ModelSaveContext context = new ModelSaveContext(saveContext.getSavePath());
    context.setCheckpoint(saveContext.getCheckpint());
    List<MatrixSaveContextProto> matricesContext = saveContext.getMatrixContextesList();
    int size = matricesContext.size();
    for (int i = 0; i < size; i++) {
      context.addMatrix(convert(matricesContext.get(i)));
    }
    return context;
  }

  public static MatrixSaveContext convert(MatrixSaveContextProto matrixSaveContext) {
    return new MatrixSaveContext(matrixSaveContext.getMatrixName(),
        matrixSaveContext.getRowIndexesList(), matrixSaveContext.getFormatClassName());
  }

  public static PSMatricesSaveResult convert(PSMatricesSaveResultProto subResultProto) {
    PSMatricesSaveResult result =
        new PSMatricesSaveResult(subResultProto.getRequestId(), subResultProto.getSubRequestId(),
            SaveState.valueOf(subResultProto.getSaveState()));
    if (subResultProto.hasFailedLog()) {
      result.setErrorMsg(subResultProto.getFailedLog());
    }
    return result;
  }

  public static PSMatricesSaveResultProto convert(PSMatricesSaveResult subResult) {
    PSMatricesSaveResultProto.Builder builder = PSMatricesSaveResultProto.newBuilder();
    builder.setRequestId(subResult.getRequestId()).setSubRequestId(subResult.getSubRequestId())
        .setSaveState(subResult.getState().getStateId());
    if (subResult.getErrorMsg() != null) {
      builder.setFailedLog(subResult.getErrorMsg());
    }
    return builder.build();
  }

  public static PSMatricesSaveContextProto convert(PSMatricesSaveContext subSaveRequest) {
    PSMatricesSaveContextProto.Builder matricesBuilder = PSMatricesSaveContextProto.newBuilder();
    matricesBuilder.setRequestId(subSaveRequest.getRequestId())
        .setSubRequestId(subSaveRequest.getSubRequestId());
    List<PSMatrixSaveContext> matrixContexts = subSaveRequest.getMatrixSaveContexts();
    int size = matrixContexts.size();
    for (int i = 0; i < size; i++) {
      matricesBuilder.addSubSaveContexts(convert(matrixContexts.get(i)));
    }
    return matricesBuilder.build();
  }

  public static PSMatricesLoadContextProto convert(PSMatricesLoadContext loadContext) {
    PSMatricesLoadContextProto.Builder builder = PSMatricesLoadContextProto.newBuilder();
    builder.setRequestId(loadContext.getRequestId()).setSubRequestId(loadContext.getSubRequestId());
    List<PSMatrixLoadContext> matrixContexts = loadContext.getMatrixLoadContexts();
    int size = matrixContexts.size();
    for (int i = 0; i < size; i++) {
      builder.addSubLoadContexts(convert(matrixContexts.get(i)));
    }

    return builder.build();
  }

  public static PSMatrixLoadContextProto convert(PSMatrixLoadContext loadContext) {
    PSMatrixLoadContextProto.Builder builder = PSMatrixLoadContextProto.newBuilder();
    builder.setMatrixId(loadContext.getMatrixId()).addAllPartIds(loadContext.getPartIds())
        .setLoadPath(loadContext.getLoadPath())
        .setFormatClassName(loadContext.getFormatClassName());
    return builder.build();
  }

  public static PSMatricesLoadContext convert(PSMatricesLoadContextProto loadContextProto) {
    List<PSMatrixLoadContext> matrixContexts =
        new ArrayList<>(loadContextProto.getSubLoadContextsCount());
    List<PSMatrixLoadContextProto> matrixContextProtos = loadContextProto.getSubLoadContextsList();
    int size = matrixContextProtos.size();
    for (int i = 0; i < size; i++) {
      matrixContexts.add(convert(matrixContextProtos.get(i)));
    }

    return new PSMatricesLoadContext(loadContextProto.getRequestId(),
        loadContextProto.getSubRequestId(), matrixContexts);
  }

  public static PSMatrixLoadContext convert(PSMatrixLoadContextProto loadContextProto) {
    return new PSMatrixLoadContext(loadContextProto.getMatrixId(), loadContextProto.getLoadPath(),
        loadContextProto.getPartIdsList(), loadContextProto.getFormatClassName());
  }

  public static PSMatricesLoadResultProto convert(PSMatricesLoadResult result) {
    PSMatricesLoadResultProto.Builder builder = PSMatricesLoadResultProto.newBuilder();
    builder.setRequestId(result.getRequestId()).setSubRequestId(result.getSubRequestId())
        .setLoadState(result.getState().getStateId()).build();
    if (result.getErrorMsg() != null) {
      builder.setFailedLog(result.getErrorMsg());
    }
    return builder.build();
  }

  public static PSMatricesLoadResult convert(PSMatricesLoadResultProto resultProto) {
    PSMatricesLoadResult result =
        new PSMatricesLoadResult(resultProto.getRequestId(), resultProto.getSubRequestId(),
            LoadState.valueOf(resultProto.getLoadState()));
    if (resultProto.hasFailedLog()) {
      result.setErrorMsg(resultProto.getFailedLog());
    }
    return result;
  }

  public static PSMatricesSaveContext convert(PSMatricesSaveContextProto saveContextProto) {
    List<PSMatrixSaveContext> matrixContexts =
        new ArrayList<>(saveContextProto.getSubSaveContextsCount());
    List<PSMatrixSaveContextProto> matrixContextProtos = saveContextProto.getSubSaveContextsList();
    int size = matrixContextProtos.size();
    for (int i = 0; i < size; i++) {
      matrixContexts.add(convert(matrixContextProtos.get(i)));
    }

    return new PSMatricesSaveContext(saveContextProto.getRequestId(),
        saveContextProto.getSubRequestId(), matrixContexts);
  }

  public static MatrixLoadContextProto convert(MatrixLoadContext context) {
    return MatrixLoadContextProto.newBuilder().setMatrixName(context.getMatrixName()).build();
  }

  public static MatrixLoadContext convert(MatrixLoadContextProto contextProto) {
    return new MatrixLoadContext(contextProto.getMatrixName());
  }

  public static ModelLoadContextProto convert(ModelLoadContext context) {
    ModelLoadContextProto.Builder builder = ModelLoadContextProto.newBuilder();
    builder.setLoadPath(context.getLoadPath());
    List<MatrixLoadContext> matrixContexts = context.getMatricesContext();
    int size = matrixContexts.size();
    for (int i = 0; i < size; i++) {
      builder.addMatrixContextes(convert(matrixContexts.get(i)));
    }
    return builder.build();
  }

  public static ModelLoadContext convert(ModelLoadContextProto contextProto) {
    List<MatrixLoadContextProto> matrixContextProtos = contextProto.getMatrixContextesList();
    int size = matrixContextProtos.size();
    List<MatrixLoadContext> matrixContexts = new ArrayList<>(size);
    for (int i = 0; i < size; i++) {
      matrixContexts.add(convert(matrixContextProtos.get(i)));
    }
    return new ModelLoadContext(contextProto.getLoadPath(), matrixContexts);
  }

  public static SaveResultProto convert(SaveResult result) {
    return SaveResultProto.newBuilder().setModelPath(result.getModelPath())
        .setMatrixPath(result.getMatrixPath()).setSaveTs(result.getSaveTs()).build();
  }

  public static SaveResult convert(SaveResultProto result) {
    return new SaveResult(result.getModelPath(), result.getMatrixPath(), result.getSaveTs());
  }
}
