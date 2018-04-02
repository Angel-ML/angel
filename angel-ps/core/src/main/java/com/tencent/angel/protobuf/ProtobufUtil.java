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
import com.tencent.angel.common.location.Location;
import com.tencent.angel.exception.StandbyException;
import com.tencent.angel.master.task.AMTask;
import com.tencent.angel.master.worker.attempt.WorkerAttempt;
import com.tencent.angel.master.worker.worker.AMWorker;
import com.tencent.angel.master.worker.workergroup.AMWorkerGroup;
import com.tencent.angel.ml.matrix.*;
import com.tencent.angel.ml.matrix.transport.PSLocation;
import com.tencent.angel.protobuf.generated.MLProtos;
import com.tencent.angel.protobuf.generated.MLProtos.*;
import com.tencent.angel.protobuf.generated.PSMasterServiceProtos;
import com.tencent.angel.protobuf.generated.WorkerMasterServiceProtos.*;
import com.tencent.angel.protobuf.generated.WorkerMasterServiceProtos.GetWorkerGroupMetaInfoResponse.WorkerGroupStatus;
import com.tencent.angel.ps.PSAttemptId;
import com.tencent.angel.ps.ParameterServerId;
import com.tencent.angel.ps.Partitioner;
import com.tencent.angel.ps.impl.matrix.PartitionState;
import com.tencent.angel.ps.recovery.ha.RecoverPartKey;
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
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;

import java.io.IOException;
import java.util.ArrayList;
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
    return PSAttemptIdProto.newBuilder().setPsId(convertToIdProto(id.getPsId())).setAttemptIndex(id.getIndex()).build();
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

  public static CreateMatricesRequest buildCreateMatricesRequest(List<MatrixContext> matrices) {
    CreateMatricesRequest.Builder createMatricesReqBuilder =
      CreateMatricesRequest.newBuilder();
    if (matrices != null) {
      int size = matrices.size();
      for(int i = 0; i < size; i++) {
        createMatricesReqBuilder.addMatrices(convertToMatrixContextProto(matrices.get(i)));
      }
    }

    return createMatricesReqBuilder.build();
  }

  public static MatrixContextProto convertToMatrixContextProto(MatrixContext mContext) {
    return MatrixContextProto.newBuilder().setName(mContext.getName())
      .setId(mContext.getMatrixId())
      .setRowNum(mContext.getRowNum())
      .setColNum(mContext.getColNum())
      .setValidIndexNum(mContext.getValidIndexNum())
      .setBlockRowNum(mContext.getMaxRowNumInBlock())
      .setBlockColNum(mContext.getMaxColNumInBlock())
      .setRowType(mContext.getRowType().getNumber())
      .setPartitionerClassName(mContext.getPartitionerClass().getName())
      .addAllAttribute(convertToPairs(mContext.getAttributes()))
      .build();
  }

  public static List<Pair> convertToPairs(Map<String, String> attrs) {
    Pair.Builder builder = Pair.newBuilder();
    List<Pair> pairs = new ArrayList<>(attrs.size());
    for(Entry<String, String> kv:attrs.entrySet()) {
      pairs.add(builder.setKey(kv.getKey()).setValue(kv.getValue()).build());
    }
    return pairs;
  }

  public static Map<String, String> convertToAttributes(List<Pair> pairs) {
    int size = pairs.size();
    Map<String, String> attrs = new HashMap<>(size);

    for(int i = 0; i < size; i++) {
      attrs.put(pairs.get(i).getKey(), pairs.get(i).getValue());
    }
    return attrs;
  }

  public static List<MatrixContext> convertToMatrixContexts(List<MatrixContextProto> matrixContextProtoList)
    throws ClassNotFoundException {
    List<MatrixContext> matrixContexts = new ArrayList<>(matrixContextProtoList.size());
    int size = matrixContextProtoList.size();
    for(int i = 0; i < size; i++) {
      matrixContexts.add(convertToMatrixContext(matrixContextProtoList.get(i)));
    }
    return matrixContexts;
  }

  public static MatrixContext convertToMatrixContext(MatrixContextProto matrixContextProto)
    throws ClassNotFoundException {
    MatrixContext matrixContext = new MatrixContext(matrixContextProto.getName(), matrixContextProto.getRowNum(),
      matrixContextProto.getColNum(), matrixContextProto.getValidIndexNum(), matrixContextProto.getBlockRowNum(), matrixContextProto.getBlockColNum(),
      RowType.valueOf(matrixContextProto.getRowType()));

    matrixContext.setMatrixId(matrixContextProto.getId());
    matrixContext.setPartitionerClass(
      (Class<? extends Partitioner>) Class.forName(matrixContextProto.getPartitionerClassName()));
    matrixContext.getAttributes().putAll(convertToAttributes(matrixContextProto.getAttributeList()));

    return matrixContext;
  }

  public static MatrixMetaProto convertToMatrixMetaProto(MatrixMeta matrixMeta) {
    MatrixMetaProto.Builder builder = MatrixMetaProto.newBuilder();
    builder.setMatrixContext(convertToMatrixContextProto(matrixMeta.getMatrixContext()));
    Map<Integer, PartitionMeta> matrixMetas = matrixMeta.getPartitionMetas();

    for(Entry<Integer, PartitionMeta> entry : matrixMetas.entrySet()) {
      builder.addPartMetas(convertToParitionMetaProto(entry.getValue()));
    }
    return builder.build();
  }

  public static List<PSIdProto> convertToPSIdProtos(List<ParameterServerId> psIds) {
    int size = psIds.size();
    List<PSIdProto> psIdProtos = new ArrayList<>(size);
    for(int i = 0; i < size; i++) {
      psIdProtos.add(convertToIdProto(psIds.get(i)));
    }
    return psIdProtos;
  }

  public static List<ParameterServerId> convertToPSIds(List<PSIdProto> psIdProtos) {
    int size = psIdProtos.size();
    List<ParameterServerId> psIds = new ArrayList<>(size);
    for(int i = 0; i < size; i++) {
      psIds.add(convertToId(psIdProtos.get(i)));
    }
    return psIds;
  }

  public static PartitionMetaProto convertToParitionMetaProto(PartitionMeta partitionMeta) {
    PartitionMetaProto.Builder builder = PartitionMetaProto.newBuilder();
    builder.setPartitionId(partitionMeta.getPartId())
      .setStartRow(partitionMeta.getStartRow())
      .setEndRow(partitionMeta.getEndRow())
      .setStartCol(partitionMeta.getStartCol())
      .setEndCol(partitionMeta.getEndCol())
      .addAllStoredPs(convertToPSIdProtos(partitionMeta.getPss()));

    return builder.build();
  }

  public static PSLocationProto convertToPSLocProto(ParameterServerId psId, Location location) {
    if(location == null) {
      return PSLocationProto.newBuilder().setPsId(convertToIdProto(psId)).setPsStatus(PSStatus.PS_NOTREADY).build();
    } else {
      return PSLocationProto.newBuilder().setPsId(convertToIdProto(psId)).setLocation(convertToLocationProto(location)).setPsStatus(PSStatus.PS_OK).build();
    }
  }

  public static LocationProto convertToLocationProto(Location location) {
    return LocationProto.newBuilder().setIp(location.getIp()).setPort(location.getPort()).build();
  }

  public static MatrixMeta convertToMatrixMeta(MatrixMetaProto matrixMetaProto)
    throws ClassNotFoundException {
    MatrixContext matrixContext = convertToMatrixContext(matrixMetaProto.getMatrixContext());
    MatrixMeta matrixMeta = new MatrixMeta(matrixContext);
    List<PartitionMetaProto> partMetaProtos = matrixMetaProto.getPartMetasList();
    int size = partMetaProtos.size();
    for(int i = 0; i < size; i++) {
      matrixMeta.addPartitionMeta(partMetaProtos.get(i).getPartitionId(),
        convertToParitionMeta(matrixContext.getMatrixId(), partMetaProtos.get(i)));
    }
    return matrixMeta;
  }

  public static List<MatrixMeta> convertToMatricesMeta(List<MatrixMetaProto> matricesMetaProto) throws ClassNotFoundException{
    int size = matricesMetaProto.size();
    List<MatrixMeta> matricesMeta = new ArrayList<>(size);
    for(int i = 0; i < size; i++) {
      matricesMeta.add(convertToMatrixMeta(matricesMetaProto.get(i)));
    }
    return matricesMeta;
  }

  public static PartitionMeta convertToParitionMeta(int matrixId, PartitionMetaProto partMetaProto) {
    return new PartitionMeta(matrixId, partMetaProto.getPartitionId(), partMetaProto.getStartRow(), partMetaProto.getEndRow(),
      partMetaProto.getStartCol(), partMetaProto.getEndCol(), convertToPSIds(partMetaProto.getStoredPsList()));
  }

  public static Location convertToLocation(PSLocationProto psLocation) {
    if(psLocation.getPsStatus() == PSStatus.PS_NOTREADY) {
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

  public static List<MatrixReport> convertToMatrixReports(List<PSMasterServiceProtos.MatrixReportProto> reportProtos) {
    int size = reportProtos.size();
    List<MatrixReport> reports = new ArrayList<>(size);
    for(int i = 0; i < size; i++) {
      reports.add(convertToMatrixReport(reportProtos.get(i)));
    }

    return reports;
  }

  public static MatrixReport convertToMatrixReport(PSMasterServiceProtos.MatrixReportProto reportProto) {
    return new MatrixReport(reportProto.getMatrixId(), convertToPartReports(reportProto.getPartReportsList()));
  }

  public static List<PartReport> convertToPartReports(List<PSMasterServiceProtos.PartReportProto> reportProtos) {
    int size = reportProtos.size();
    List<PartReport> reports = new ArrayList<>(size);
    for(int i = 0; i < size; i++) {
      reports.add(convertToPartReport(reportProtos.get(i)));
    }
    return reports;
  }

  public static PartReport convertToPartReport(PSMasterServiceProtos.PartReportProto reportProto) {
    return new PartReport(reportProto.getPartId(), PartitionState.valueOf(reportProto.getStatus()));
  }

  public static PSMasterServiceProtos.RecoverPartKeyProto convert(RecoverPartKey recoverPartKey) {
    return PSMasterServiceProtos.RecoverPartKeyProto.newBuilder()
      .setMatrixId(recoverPartKey.partKey.getMatrixId())
      .setPartId(recoverPartKey.partKey.getPartitionId())
      .setPsId(convertToIdProto(recoverPartKey.psLoc.psId))
      .setLoc(convertToLocationProto(recoverPartKey.psLoc.loc))
      .build();
  }

  public static RecoverPartKey convert(PSMasterServiceProtos.RecoverPartKeyProto proto) {
    return new RecoverPartKey(new PartitionKey(proto.getMatrixId(), proto.getPartId()),
      new PSLocation(convertToId(proto.getPsId()), convert(proto.getLoc())));
  }

  public static Location convert(LocationProto locProto) {
    return new Location(locProto.getIp(), locProto.getPort());
  }

  public static Map<Integer, List<Integer>> convertToNeedSaveMatrices(List<PSMasterServiceProtos.NeedSaveMatrixProto> protos) {
    Map<Integer, List<Integer>> needSaveMatrices = new HashMap<>(protos.size());
    for(PSMasterServiceProtos.NeedSaveMatrixProto matrixProto : protos) {
      needSaveMatrices.put(matrixProto.getMatrixId(), new ArrayList<>(matrixProto.getPartIdsList()));
    }
    return needSaveMatrices;
  }


  public static PSLocationProto convert(PSLocation psLoc) {
    return PSLocationProto.newBuilder().setPsId(convertToIdProto(psLoc.psId))
      .setLocation(convertToLocationProto(psLoc.loc)).setPsStatus(PSStatus.PS_OK).build();
  }


  public static HashMap<PSLocation, Integer> convert(PSFailedReportsProto reportsProto) {
    HashMap<PSLocation, Integer> reports = new HashMap<>();
    List<PSFailedReportProto> reportList = reportsProto.getPsFailedReportsList();
    int size = reportList.size();
    for(int i = 0; i < size; i++) {
      reports.put(new PSLocation(convertToId(reportList.get(i).getPsLoc().getPsId()),
        convert(reportList.get(i).getPsLoc().getLocation())), reportList.get(i).getFailedCounter());
    }
    return reports;
  }

  public static PSLocation convert(PSLocationProto psLoc) {
    return new PSLocation(convertToId(psLoc.getPsId()), convertToLocation(psLoc.getLocation()));
  }
}
