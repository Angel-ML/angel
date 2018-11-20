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


package com.tencent.angel.client;

import com.google.protobuf.ServiceException;
import com.tencent.angel.RunningMode;
import com.tencent.angel.common.location.Location;
import com.tencent.angel.conf.AngelConf;
import com.tencent.angel.conf.MatrixConf;
import com.tencent.angel.exception.AngelException;
import com.tencent.angel.exception.InvalidParameterException;
import com.tencent.angel.ipc.TConnectionManager;
import com.tencent.angel.master.MasterProtocol;
import com.tencent.angel.ml.matrix.MatrixContext;
import com.tencent.angel.ml.model.MLModel;
import com.tencent.angel.ml.model.PSModel;
import com.tencent.angel.model.*;
import com.tencent.angel.protobuf.ProtobufUtil;
import com.tencent.angel.protobuf.generated.ClientMasterServiceProtos;
import com.tencent.angel.protobuf.generated.ClientMasterServiceProtos.*;
import com.tencent.angel.protobuf.generated.MLProtos.*;
import com.tencent.angel.utils.HdfsUtil;
import com.tencent.angel.utils.UGITools;
import com.tencent.angel.worker.WorkerGroupId;
import com.tencent.angel.worker.WorkerId;
import com.tencent.angel.worker.task.BaseTask;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.security.UserGroupInformation;

import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.security.PrivilegedExceptionAction;
import java.text.DecimalFormat;
import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Angel application client. It provides the control interfaces for the application.
 */
public abstract class AngelClient implements AngelClientInterface {
  private static final Log LOG = LogFactory.getLog(AngelClient.class);

  /**
   * application configuration
   */
  protected final Configuration conf;

  /**
   * matrices used in the application
   */
  private final Map<String, MatrixContext> nameToMatrixMap;

  /**
   * rpc client to master
   */
  protected volatile MasterProtocol master;

  private GetJobReportRequest getJobReportReq;
  private GetJobReportRequest.Builder getJobReportReqBuilder;
  private GetJobReportResponse lastReport;
  private boolean isExecuteFinished;
  private boolean isFinished;
  private String appFailedMessage;

  /**
   * temporary file use to store application state
   */
  protected Path internalStateFile;

  /**
   * the application submitting user
   */
  protected String userName;

  /**
   * master location
   */
  protected Location masterLocation;

  private static final DecimalFormat df = new DecimalFormat("#0.000000");

  private volatile int clientId = -1;

  private volatile Thread hbThread;

  private final int hbIntervalMS;

  private final AtomicBoolean stopped = new AtomicBoolean(false);

  /**
   * Create a new AngelClient.
   *
   * @param conf application configuration
   */
  public AngelClient(Configuration conf) {
    this.conf = conf;
    nameToMatrixMap = new LinkedHashMap<>();
    isExecuteFinished = false;
    isFinished = false;
    hbIntervalMS = conf.getInt(AngelConf.ANGEL_CLIENT_HEARTBEAT_INTERVAL_MS,
      AngelConf.DEFAULT_ANGEL_CLIENT_HEARTBEAT_INTERVAL_MS);
  }

  @SuppressWarnings("rawtypes") @Override public void runTask(Class<? extends BaseTask> taskClass)
    throws AngelException {
    if (master == null) {
      throw new AngelException(
        "parameter servers are not started, you must execute startPSServer first!!");
    }

    try {
      master.setParams(null, SetParamsRequest.newBuilder().addKvs(
        Pair.newBuilder().setKey(AngelConf.ANGEL_TASK_USER_TASKCLASS).setValue(taskClass.getName())
          .build()).build());
      master.start(null, StartRequest.newBuilder().build());
    } catch (ServiceException e) {
      LOG.error("start application failed.", e);
      throw new AngelException(e);
    }
  }

  public void runTask(String taskClassName) throws AngelException {
    if (master == null) {
      throw new AngelException(
        "parameter servers are not started, you must execute startPSServer first!!");
    }

    try {
      master.setParams(null, SetParamsRequest.newBuilder().addKvs(
        Pair.newBuilder().setKey(AngelConf.ANGEL_TASK_USER_TASKCLASS).setValue(taskClassName)
          .build()).build());
      master.start(null, StartRequest.newBuilder().build());
    } catch (ServiceException e) {
      LOG.error("start application failed.", e);
      throw new AngelException(e);
    }
  }

  protected void startHeartbeat() throws ServiceException {
    if (master == null) {
      LOG.error("Master has not been connected");
      return;
    }
    clientId = master.getClientId(null, GetClientIdRequest.getDefaultInstance()).getClientId();
    master.clientRegister(null, ClientRegisterRequest.newBuilder().setClientId(clientId).build());

    LOG.info("clientId=" + clientId);
    hbThread = new Thread(() -> {
      while (!stopped.get() && !Thread.interrupted()) {
        try {
          Thread.sleep(hbIntervalMS);
          master.keepAlive(null, KeepAliveRequest.newBuilder().setClientId(clientId).build());
        } catch (Throwable e) {
          if (!stopped.get()) {
            LOG.error("AngelClient " + clientId + " send heartbeat to Master failed ", e);
          }
        }
      }
    });

    hbThread.setName("client-heartbeat");
    hbThread.start();
  }

  @Override public void run() throws AngelException {
    if (master == null) {
      throw new AngelException(
        "parameter servers are not started, you must execute startPSServer first!!");
    }

    createMatrices();

    try {
      master.start(null, StartRequest.newBuilder().build());
    } catch (ServiceException e) {
      LOG.error("start application failed.", e);
      throw new AngelException(e);
    }
  }

  @Override public void addMatrix(MatrixContext mContext) throws AngelException {
    if (nameToMatrixMap.containsKey(mContext.getName())) {
      throw new AngelException(
        "Matrix \"" + mContext.getName() + "\" already exist, please check it");
    }

    try {
      nameToMatrixMap.put(mContext.getName(), mContext);
    } catch (Throwable x) {
      throw new AngelException(x);
    }
  }

  @SuppressWarnings("rawtypes") @Override public void loadModel(MLModel model)
    throws AngelException {
    if (master == null) {
      throw new AngelException(
        "parameter servers are not started, you must execute startPSServer first!!");
    }

    Map<String, PSModel> psModels = model.getPSModels();
    for (Map.Entry<String, PSModel> entry : psModels.entrySet()) {
      addMatrix(entry.getValue().getContext());
    }

    createMatrices();
    load(psModels.keySet());
  }

  @SuppressWarnings("rawtypes") @Override public void saveModel(MLModel model)
    throws AngelException {
    if (master == null) {
      throw new AngelException(
        "parameter servers are not started, you must execute startPSServer first!!");
    }

    Map<String, PSModel> psModels = model.getPSModels();
    ModelSaveContext saveContext = new ModelSaveContext();

    for (Map.Entry<String, PSModel> entry : psModels.entrySet()) {
      MatrixContext context = entry.getValue().getContext();
      String savePath = context.getAttributes().get(MatrixConf.MATRIX_SAVE_PATH);
      if (savePath != null) {
        saveContext.addMatrix(new MatrixSaveContext(context.getName()));
      }
    }
    saveContext.setSavePath(conf.get(AngelConf.ANGEL_JOB_OUTPUT_PATH));
    save(saveContext);
    LOG.info("save is finish");
  }

  /**
   * Save matrices to files.
   *
   * @param matrixNames need save matrix name list
   */
  public void saveMatrices(List<String> matrixNames) {
    ModelSaveContext saveContext = new ModelSaveContext();
    for (String name : matrixNames) {
      saveContext.addMatrix(new MatrixSaveContext(name));
    }
    save(saveContext);
  }

  @Override public void save(ModelSaveContext saveContext) throws AngelException {
    if (saveContext.getMatricesContext().size() == 0 || saveContext.getSavePath() == null
      || saveContext.getSavePath().isEmpty()) {
      LOG.info("there is no matrices need save or save path is empty");
      return;
    }

    try {
      /*UserGroupInformation ugi = UGITools.getCurrentUser(conf);
      ugi.doAs(new PrivilegedExceptionAction<String>() {
        @Override public String run() throws Exception {
          Path savePath = new Path(saveContext.getSavePath());
          FileSystem fs = savePath.getFileSystem(conf);
          if(fs.exists(savePath)) {
            if(conf.getBoolean(AngelConf.ANGEL_JOB_OUTPUT_PATH_DELETEONEXIST,
                    AngelConf.DEFAULT_ANGEL_JOB_OUTPUT_PATH_DELETEONEXIST)) {
              fs.delete(savePath, true);
            } else {
              throw new AngelException("Save path " + savePath + " already exist, you can set another save path or set angel.job.output.path.deleteonexist be true");
            }
          }
          return "OK";
        }
      });
      */
      Path savePath = new Path(saveContext.getSavePath());
      FileSystem fs = savePath.getFileSystem(conf);
      if(fs.exists(savePath)) {
        if(conf.getBoolean(AngelConf.ANGEL_JOB_OUTPUT_PATH_DELETEONEXIST,
                AngelConf.DEFAULT_ANGEL_JOB_OUTPUT_PATH_DELETEONEXIST)) {
          fs.delete(savePath, true);
          if(fs.exists(savePath)) {
            throw new AngelException("Save path " + savePath + " already exist, remove it failed!!!");
          }
        } else {
          throw new AngelException("Save path " + savePath + " already exist, you can set another save path or set angel.job.output.path.deleteonexist be true");
        }
      }
    } catch (Throwable x) {
      throw new AngelException(x);
    }

    SaveRequest.Builder builder = SaveRequest.newBuilder();
    int requestId;
    try {
      requestId =
        master.save(null, builder.setSaveContext(ProtobufUtil.convert(saveContext)).build())
          .getRequestId();
    } catch (ServiceException e) {
      LOG.error("save model failed.", e);
      throw new AngelException(e);
    }

    while (!isSaveCompleted(requestId)) {
      try {
        Thread.sleep(1000);
      } catch (InterruptedException e) {
        throw new AngelException("Interrupted as waiting app complete");
      }
    }

    if (appFailedMessage != null) {
      throw new AngelException("app run failed, " + appFailedMessage);
    }
  }

  @Override public void load(ModelLoadContext loadContext) throws AngelException {
    if (loadContext.getMatricesContext().size() == 0 || loadContext.getLoadPath() == null
      || loadContext.getLoadPath().isEmpty()) {
      LOG.error("there is no matrices need load or load path is empty");
      LOG.error(String.format("matrix context size=%d load path = %s",
              loadContext.getMatricesContext().size(),
              loadContext.getLoadPath()));
      return;
    }

    try {
      /* UserGroupInformation ugi = UGITools.getCurrentUser(conf);
      ugi.doAs(new PrivilegedExceptionAction<String>() {
        @Override public String run() throws Exception {
          Path loadPath = new Path(loadContext.getLoadPath());
          FileSystem fs = loadPath.getFileSystem(conf);
          if(!fs.exists(loadPath)) {
            throw new AngelException("Load path " + loadPath + " does not exist, please check");
          }
          return "OK";
        }
      });
      */
      Path loadPath = new Path(loadContext.getLoadPath());
      FileSystem fs = loadPath.getFileSystem(conf);
      if(!fs.exists(loadPath)) {
        throw new AngelException("Load path " + loadPath + " does not exist, please check");
      }
    } catch (Throwable e) {
      throw new AngelException(e);
    }

    LoadRequest.Builder builder = LoadRequest.newBuilder();
    int requestId;
    try {
      requestId =
        master.load(null, builder.setLoadContext(ProtobufUtil.convert(loadContext)).build())
          .getRequestId();
    } catch (ServiceException e) {
      LOG.error("save model failed.", e);
      throw new AngelException(e);
    }

    while (!isLoadCompleted(requestId)) {
      try {
        Thread.sleep(1000);
      } catch (InterruptedException e) {
        throw new AngelException("Interrupted as waiting app complete");
      }
    }

    LOG.info("load model complete from path " + loadContext.getLoadPath());

    if (appFailedMessage != null) {
      throw new AngelException("app run failed, " + appFailedMessage);
    }
  }

  @Override public void stop(int stateCode) throws AngelException {
    LOG.info("stop the application");
    stopService();
    if (master != null) {
      try {
        LOG.info("master is not null, send stop command to Master, stateCode=" + stateCode);
        master.stop(null,
          ClientMasterServiceProtos.StopRequest.newBuilder().setExitStatus(stateCode).build());
      } catch (Throwable e) {
        LOG.error("send stop command to Master failed ", e);
        kill();
        //throw new AngelException(e);
      }
    } else {
      LOG.info("master is null, just kill the application");
      kill();
    }
    close();
  }

  @Override public void stop() throws AngelException {
    stop(0);
  }

  private void stopService() {
    nameToMatrixMap.clear();
    isExecuteFinished = false;
    isFinished = false;
    if (!stopped.getAndSet(true)) {
      if (hbThread != null) {
        hbThread.interrupt();
        try {
          hbThread.join();
        } catch (Throwable e) {

        }
      }
    }
    stopped.set(false);
  }

  @Override public void waitForCompletion() throws AngelException {
    if (master == null) {
      throw new AngelException(
        "parameter servers are not started, you must execute startPSServer first!!");
    }

    RunningMode mode = RunningMode
      .valueOf(conf.get(AngelConf.ANGEL_RUNNING_MODE, AngelConf.DEFAULT_ANGEL_RUNNING_MODE));
    switch (mode) {
      case ANGEL_PS: {
        while (!isCompleted()) {
          try {
            Thread.sleep(1000);
          } catch (InterruptedException e) {
            throw new AngelException("Interrupted as waiting app complete");
          }
        }

        if (appFailedMessage != null) {
          throw new AngelException("app run failed, " + appFailedMessage);
        }
        break;
      }
      case ANGEL_PS_WORKER: {
        printWorkerLogUrl(new WorkerId(new WorkerGroupId(0), 0));
        while (!isExecuteCompleted()) {
          try {
            Thread.sleep(1000);
          } catch (InterruptedException e) {
            throw new AngelException("Interrupted as waiting app complete");
          }
        }

        if (appFailedMessage != null) {
          throw new AngelException("app run failed, " + appFailedMessage);
        }

        String actionType =
          conf.get(AngelConf.ANGEL_ACTION_TYPE, AngelConf.DEFAULT_ANGEL_ACTION_TYPE);
        LOG.info("action type " + actionType);
        if (actionType.matches("predict")) {
          try {
            movePredictResult();
          } catch (IOException e) {
            throw new AngelException("move predict result failed." + e.getMessage());
          }
        }
        break;
      }

      default:
        break;
    }
  }

  private void movePredictResult() throws IOException {
    String outPathStr = conf.get(AngelConf.ANGEL_JOB_OUTPUT_PATH);
    String tmpPathStr = conf.get(AngelConf.ANGEL_JOB_TMP_OUTPUT_PATH);
    Path outPath = new Path(outPathStr);
    Path tmpOutPath = new Path(tmpPathStr, "predict");

    FileSystem fs = outPath.getFileSystem(conf);
    Path tmpCombinePath = HdfsUtil.toTmpPath(outPath);
    HdfsUtil.copyFilesInSameHdfs(tmpOutPath, tmpCombinePath, fs);
    LOG.info("copy files from " + tmpOutPath + " to " + tmpCombinePath);
    HdfsUtil.rename(tmpCombinePath, outPath, fs);
    LOG.info("rename " + tmpCombinePath + " to " + outPath);
  }

  public Path getInternalStateFile() {
    return internalStateFile;
  }

  /**
   * Get applicaiton configuration.
   *
   * @return Configuration the applicaiton configuration
   */
  public Configuration getConf() {
    return conf;
  }

  /**
   * Get the location(ip:port) of the application master.
   *
   * @return Location the location(ip:port) of the application master
   */
  public Location getMasterLocation() {
    return masterLocation;
  }


  /**
   * Check the application is over or not. If the application state is J_SUCCESSED, J_FAILED or
   * J_KILLED, means it is over.
   *
   * @return boolean true means the application is over
   */
  private boolean isCompleted() {
    if (isFinished) {
      return true;
    }
    updateJobReport();

    if (lastReport == null) {
      try {
        lastReport = tryGetResponseFromFile(true);
        LOG.info("app from file is " + lastReport);
      } catch (IOException e) {
        LOG.error("get app from file failed ", e);
      }
    }

    if (lastReport == null || lastReport.getJobReport().getJobState() == JobStateProto.J_NEW) {
      appFailedMessage = " detail is killed";
      return true;
    }

    JobStateProto jobState = lastReport.getJobReport().getJobState();
    if (LOG.isDebugEnabled()) {
      LOG.debug("job stat = " + jobState.name());
    }
    if (jobState == JobStateProto.J_SUCCEEDED || jobState == JobStateProto.J_FAILED
      || jobState == JobStateProto.J_KILLED) {
      isFinished = true;
      LOG.info("job is finished! status: " + jobState);
      if (jobState == JobStateProto.J_FAILED || jobState == JobStateProto.J_KILLED) {

        appFailedMessage = " detail is " + lastReport.getJobReport().getDiagnostics();
        LOG.error(appFailedMessage);
      }
      return true;
    } else {
      return false;
    }
  }

  /**
   * Check a save request complete or not
   *
   * @param requestId save request id
   * @return true means complete
   * @throws AngelException
   */
  private boolean isSaveCompleted(int requestId) throws AngelException {
    CheckModelSavedResponse response;
    try {
      response = master
        .checkModelSaved(null, CheckModelSavedRequest.newBuilder().setRequestId(requestId).build());
    } catch (Throwable x) {
      throw new AngelException("Check model save request failed ", x);
    }

    SaveState state = SaveState.valueOf(response.getStatus());
    if (state == SaveState.FAILED) {
      throw new AngelException("Model save falied " + response.getLog());
    } else if (state == SaveState.SUCCESS) {
      return true;
    } else {
      return false;
    }
  }

  /**
   * Check a load request complete or not
   *
   * @param requestId load request id
   * @return true means complete
   * @throws AngelException
   */
  private boolean isLoadCompleted(int requestId) throws AngelException {
    CheckModelLoadedResponse response;
    try {
      response = master.checkModelLoaded(null,
        CheckModelLoadedRequest.newBuilder().setRequestId(requestId).build());
    } catch (Throwable x) {
      throw new AngelException("Check model save request failed ", x);
    }

    LoadState state = LoadState.valueOf(response.getStatus());
    if (state == LoadState.FAILED) {
      throw new AngelException("Model load falied " + response.getLog());
    } else if (state == LoadState.SUCCESS) {
      return true;
    } else {
      return false;
    }
  }

  /**
   * Check the application calculation phase is over or not. If the application state is J_SUCCESSED, J_FAILED
   * J_KILLED, J_EXECUTE_SUCCESSED or J_COMMITTING means it is over.
   *
   * @return boolean true means the application is over
   */
  private boolean isExecuteCompleted() {
    if (isExecuteFinished) {
      return true;
    }
    updateJobReport();

    if (lastReport == null) {
      try {
        lastReport = tryGetResponseFromFile(true);
        LOG.info("app from file is " + lastReport);
      } catch (IOException e) {
        LOG.error("get app from file failed ", e);
      }
    }

    if (lastReport == null || lastReport.getJobReport().getJobState() == JobStateProto.J_NEW) {
      appFailedMessage = " detail is killed";
      return true;
    }

    JobStateProto jobState = lastReport.getJobReport().getJobState();
    if (LOG.isDebugEnabled()) {
      LOG.debug("job stat = " + jobState.name());
    }
    if (jobState != JobStateProto.J_INITED && jobState != JobStateProto.J_NEW
      && jobState != JobStateProto.J_RUNNING) {
      isExecuteFinished = true;
      LOG.info("job is finished! status: " + jobState);
      if (jobState == JobStateProto.J_FAILED || jobState == JobStateProto.J_KILLED) {

        appFailedMessage = " detail is " + lastReport.getJobReport().getDiagnostics();
        LOG.error(appFailedMessage);
      }
      return true;
    } else {
      return false;
    }
  }

  protected void printWorkerLogUrl(WorkerId workerId) {
  }

  private void updateJobReport() {
    GetJobReportRequest getJobRequest = getGetJobReportRequest();
    GetJobReportResponse response = null;
    try {
      response = master.getJobReport(null, getJobRequest);
    } catch (Exception e) {
      LOG.error("getJobReport from master failed. " + e.getMessage());
      try {
        updateMaster(60);
        if (master != null) {
          response = master.getJobReport(null, getJobRequest);
        }
      } catch (Exception e1) {
        LOG.error("update master failed.", e1);
      }
    }

    if (response == null) {
      isFinished = true;
      lastReport = null;
      return;
    }

    JobReportProto report = response.getJobReport();
    // JobStateProto jobState = report.getJobState();
    if (lastReport == null || (report.hasCurIteration() && report.getCurIteration() != lastReport
      .getJobReport().getCurIteration())) {
      LOG.info(
        "Epoch: " + report.getCurIteration() + ". Metrics=" + toString(report.getMetricsList()));
      if (report.hasLoss()) {
        LOG.info("loss/success: " + report.getLoss() + "/" + report.getSuccess());
      }
    }
    lastReport = response;
  }

  private String toString(List<Pair> metrics) {
    StringBuilder sb = new StringBuilder("{");
    int size = metrics.size();
    for (int i = 0; i < size; i++) {
      sb.append("\"" + metrics.get(i).getKey() + "\":" + df
        .format(Double.valueOf(metrics.get(i).getValue())));
      if (i < size - 1) {
        sb.append(",");
      }
    }
    sb.append("}");
    return sb.toString();
  }

  private GetJobReportRequest getGetJobReportRequest() {
    if (getJobReportReq != null) {
      return getJobReportReq;
    }

    if (getJobReportReqBuilder == null) {
      getJobReportReqBuilder = GetJobReportRequest.newBuilder();
    }

    getJobReportReqBuilder.setAppId(getAppId());
    return getJobReportReqBuilder.build();
  }

  private GetJobReportResponse tryGetResponseFromFile(boolean deleteOnExist) throws IOException {
    GetJobReportResponse response = null;
    FileSystem fs = internalStateFile.getFileSystem(conf);
    if (fs.exists(internalStateFile)) {
      LOG.info(internalStateFile + " exist, parse app report from it");
      FSDataInputStream in = fs.open(internalStateFile);
      response = GetJobReportResponse.parseFrom(in);

      if (deleteOnExist) {
        fs.delete(internalStateFile.getParent(), true);
      }
    }

    return response;
  }

  @Override public void createMatrices() throws AngelException {
    try {
      for (MatrixContext context : nameToMatrixMap.values()) {
        context.init(conf);
      }

      master.createMatrices(null,
        ProtobufUtil.buildCreateMatricesRequest(new ArrayList<>(nameToMatrixMap.values())));
      List<String> matrixNames = new ArrayList<>(nameToMatrixMap.keySet());
      waitForMatricesCreated(matrixNames);
    } catch (Throwable x) {
      throw new AngelException(x);
    }
  }

  @Override public void createMatrices(List<MatrixContext> matrixContexts) throws AngelException {
    try {
      for (MatrixContext context : matrixContexts) {
        context.init(conf);
      }

      master.createMatrices(null, ProtobufUtil.buildCreateMatricesRequest(matrixContexts));
      List<String> matrixNames = new ArrayList<>(matrixContexts.size());
      for (MatrixContext context : matrixContexts) {
        matrixNames.add(context.getName());
      }
      waitForMatricesCreated(matrixNames);
    } catch (Throwable x) {
      throw new AngelException(x);
    }
  }

  public void load() {
    load(nameToMatrixMap.keySet());
  }

  public void load(Set<String> matrixNames) {
    // Check need load matrices
    String loadPath = conf.get(AngelConf.ANGEL_LOAD_MODEL_PATH);
    if (loadPath != null && !loadPath.isEmpty()) {
      ModelLoadContext loadContext = new ModelLoadContext(loadPath);
      int needLoadMatrixCount = 0;
      for (String name : matrixNames) {
        MatrixContext matrix = nameToMatrixMap.get(name);
        if (matrix.getAttributes().get(MatrixConf.MATRIX_LOAD_PATH) != null) {
          loadContext.addMatrix(new MatrixLoadContext(name));
          needLoadMatrixCount++;
        }
      }

      if (needLoadMatrixCount > 0) {
        load(loadContext);
      }
    }
  }

  private void waitForMatricesCreated(List<String> matrixNames) throws ServiceException {
    CheckMatricesCreatedRequest request =
      CheckMatricesCreatedRequest.newBuilder().addAllMatrixNames(matrixNames).build();

    int size = matrixNames.size();
    while (true) {
      CheckMatricesCreatedResponse response = master.checkMatricesCreated(null, request);
      if (response.getStatus() == 0) {
        return;
      }

      try {
        Thread.sleep(1000);
      } catch (InterruptedException e) {
        LOG.warn("waitForMatricesCreated is interrupted.");
      }
    }
  }

  protected void setInputDirectory() throws IOException {
    boolean isUseDummy = conf.getBoolean(AngelConf.ANGEL_AM_USE_DUMMY_DATASPLITER,
      AngelConf.DEFAULT_ANGEL_AM_USE_DUMMY_DATASPLITER);
    if (isUseDummy) {
      return;
    }

    String actionType = conf.get(AngelConf.ANGEL_ACTION_TYPE, AngelConf.DEFAULT_ANGEL_ACTION_TYPE);
    RunningMode runningMode = RunningMode
      .valueOf(conf.get(AngelConf.ANGEL_RUNNING_MODE, AngelConf.DEFAULT_ANGEL_RUNNING_MODE));
    String path;
    if (actionType.matches("predict")) {
      path = conf.get(AngelConf.ANGEL_PREDICT_DATA_PATH);
    } else {
      path = conf.get(AngelConf.ANGEL_TRAIN_DATA_PATH);
    }

    if (runningMode == RunningMode.ANGEL_PS_WORKER) {
      if (path == null) {
        throw new IOException("input data directory is empty, you should set it");
      } else {
        conf.set(AngelConf.ANGEL_JOB_INPUT_PATH, path);
      }
    }
  }

  protected void setOutputDirectory() throws IOException {
    String actionType = conf.get(AngelConf.ANGEL_ACTION_TYPE, AngelConf.DEFAULT_ANGEL_ACTION_TYPE);
    RunningMode runningMode = RunningMode
      .valueOf(conf.get(AngelConf.ANGEL_RUNNING_MODE, AngelConf.DEFAULT_ANGEL_RUNNING_MODE));
    LOG.info("running mode = " + runningMode);
    boolean deleteOnExist = conf.getBoolean(AngelConf.ANGEL_JOB_OUTPUT_PATH_DELETEONEXIST,
      AngelConf.DEFAULT_ANGEL_JOB_OUTPUT_PATH_DELETEONEXIST);

    String path = null;
    if (actionType.matches("train") || actionType.matches("inctrain")) {
      path = conf.get(AngelConf.ANGEL_SAVE_MODEL_PATH);
    } else if (actionType.matches("predict")) {
      path = conf.get(AngelConf.ANGEL_PREDICT_PATH);
    } else if (actionType.matches("serving")) {
      path = conf.get(AngelConf.ANGEL_SERVING_TEMP_PATH);
    } else {
      path = null;
    }

    if (path == null) {
      throw new IOException(
        "output directory is null. you must set " + AngelConf.ANGEL_SAVE_MODEL_PATH
          + " at training mode or set " + AngelConf.ANGEL_PREDICT_PATH + " at predict mode"
          + AngelConf.ANGEL_SERVING_TEMP_PATH + "at serving mode");
    }
    conf.set(AngelConf.ANGEL_JOB_OUTPUT_PATH, path);

    Path outputPath = new Path(path);
    FileSystem outFs = outputPath.getFileSystem(conf);
    if (outFs.exists(outputPath)) {
      if (deleteOnExist) {
        outFs.delete(outputPath, true);
        if(outFs.exists(outputPath)) {
          throw new IOException("output path " + outputPath + " already exist, remove it failed!!!");
        }
      } else {
        throw new IOException("output path " + outputPath + " already exist, please check");
      }
    }

    Path outputParentPath = outputPath.getParent();
    if (!outFs.exists(outputParentPath)) {
      LOG.info("Make dir for model output parent path: " + outputParentPath);
      if (!outFs.mkdirs(outputParentPath)) {
        throw new IOException(
          "Failed to make dir for model output parent path: " + outputParentPath);
      }
    }

    if (runningMode == RunningMode.ANGEL_PS_WORKER) {
      String logPathStr = conf.get(AngelConf.ANGEL_LOG_PATH);
      if (logPathStr != null) {
        Path logPath = new Path(logPathStr);
        FileSystem logFs = logPath.getFileSystem(conf);
        if (logFs.exists(logPath)) {
          if (deleteOnExist) {
            logFs.delete(logPath, true);
          } else {
            throw new IOException("log path " + logPath + " already exist, please check");
          }
        }
      }
    }

    Path tmpOutputPath = HdfsUtil.generateTmpDirectory(conf, getAppId(), outputPath);

    internalStateFile =
      new Path(HdfsUtil.generateTmpDirectory(conf, getAppId(), outputPath), "state");

    conf.set(AngelConf.ANGEL_JOB_TMP_OUTPUT_PATH, tmpOutputPath.toString());
    LOG.info(AngelConf.ANGEL_JOB_TMP_OUTPUT_PATH + "=" + tmpOutputPath.toString());

    LOG.info("internal state file is " + internalStateFile);
    conf.set(AngelConf.ANGEL_APP_SERILIZE_STATE_FILE, internalStateFile.toString());

  }

  protected void setUser()
    throws ClassNotFoundException, NoSuchFieldException, SecurityException, InstantiationException,
    IllegalAccessException, IOException {
    userName = UGITools.getCurrentUser(conf).getShortUserName();
    conf.set(AngelConf.USER_NAME, userName);
  }

  abstract public void startPSServer() throws AngelException;

  protected void setLocalAddr() throws UnknownHostException {
    InetAddress ip = InetAddress.getLocalHost();
    if (ip != null) {
      String submitHostAddress = ip.getHostAddress();
      String submitHostName = ip.getHostName();
      conf.set(AngelConf.JOB_SUBMITHOST, submitHostName);
      conf.set(AngelConf.JOB_SUBMITHOSTADDR, submitHostAddress);
    }
  }

  protected void handleDeprecatedParameters(Configuration conf) {
    String memoryMBStr = conf.get(AngelConf.ANGEL_AM_MEMORY_MB);
    String memoryGBStr = conf.get(AngelConf.ANGEL_AM_MEMORY_GB);

    if (memoryGBStr == null && memoryMBStr != null) {
      LOG.warn("use deprecated parameter " + AngelConf.ANGEL_AM_MEMORY_MB + ", you can use "
        + AngelConf.ANGEL_AM_MEMORY_GB + " instead.");

      try {
        int memoryMB = Integer.valueOf(memoryMBStr);
        conf.setInt(AngelConf.ANGEL_AM_MEMORY_GB, (int) Math.ceil((float) memoryMB / 1024));
      } catch (Exception x) {
        LOG.error("invalid value for  " + AngelConf.ANGEL_AM_MEMORY_MB + " " + memoryMBStr);
      }
    }

    memoryMBStr = conf.get(AngelConf.ANGEL_WORKER_MEMORY_MB);
    memoryGBStr = conf.get(AngelConf.ANGEL_WORKER_MEMORY_GB);

    if (memoryGBStr == null && memoryMBStr != null) {
      LOG.warn("use deprecated parameter " + AngelConf.ANGEL_WORKER_MEMORY_MB + ", you can use "
        + AngelConf.ANGEL_WORKER_MEMORY_GB + " instead.");

      try {
        int memoryMB = Integer.valueOf(memoryMBStr);
        conf.setInt(AngelConf.ANGEL_WORKER_MEMORY_GB, (int) Math.ceil((float) memoryMB / 1024));
      } catch (Exception x) {
        LOG.error("invalid value for  " + AngelConf.ANGEL_WORKER_MEMORY_MB + " " + memoryGBStr);
      }
    }

    memoryMBStr = conf.get(AngelConf.ANGEL_PS_MEMORY_MB);
    memoryGBStr = conf.get(AngelConf.ANGEL_PS_MEMORY_GB);

    if (memoryGBStr == null && memoryMBStr != null) {
      LOG.warn("use deprecated parameter " + AngelConf.ANGEL_PS_MEMORY_MB + ", you can use "
        + AngelConf.ANGEL_PS_MEMORY_GB + " instead.");

      try {
        int memoryMB = Integer.valueOf(memoryMBStr);
        conf.setInt(AngelConf.ANGEL_PS_MEMORY_GB, (int) Math.ceil((float) memoryMB / 1024));
      } catch (Exception x) {
        LOG.error("invalid value for  " + AngelConf.ANGEL_PS_MEMORY_MB + " " + memoryGBStr);
      }
    }
  }

  protected void checkParameters(Configuration conf) throws InvalidParameterException {
    StringBuilder sb = new StringBuilder();
    int coreNum = conf.getInt(AngelConf.ANGEL_AM_CPU_VCORES, AngelConf.DEFAULT_ANGEL_AM_CPU_VCORES);
    if (coreNum <= 0) {
      sb.append(AngelConf.ANGEL_AM_CPU_VCORES + " should > 0");
      sb.append("\n");
    }

    int memNum = conf.getInt(AngelConf.ANGEL_AM_MEMORY_GB, AngelConf.DEFAULT_ANGEL_AM_MEMORY_GB);
    if (memNum <= 0) {
      sb.append(AngelConf.ANGEL_AM_MEMORY_GB + " should > 0");
      sb.append("\n");
    }

    coreNum =
      conf.getInt(AngelConf.ANGEL_WORKER_CPU_VCORES, AngelConf.DEFAULT_ANGEL_WORKER_CPU_VCORES);
    if (coreNum <= 0) {
      sb.append(AngelConf.ANGEL_WORKER_CPU_VCORES + " should > 0");
      sb.append("\n");
    }

    memNum =
      conf.getInt(AngelConf.ANGEL_WORKER_MEMORY_GB, AngelConf.DEFAULT_ANGEL_WORKER_MEMORY_GB);
    if (memNum <= 0) {
      sb.append(AngelConf.ANGEL_WORKER_MEMORY_GB + " should > 0");
      sb.append("\n");
    }

    coreNum = conf.getInt(AngelConf.ANGEL_PS_CPU_VCORES, AngelConf.DEFAULT_ANGEL_PS_CPU_VCORES);
    if (coreNum <= 0) {
      sb.append(AngelConf.ANGEL_PS_CPU_VCORES + " should > 0");
      sb.append("\n");
    }

    memNum = conf.getInt(AngelConf.ANGEL_PS_MEMORY_GB, AngelConf.DEFAULT_ANGEL_PS_MEMORY_GB);
    if (memNum <= 0) {
      sb.append(AngelConf.ANGEL_PS_MEMORY_GB + " should > 0");
      sb.append("\n");
    }

    if (sb.length() > 0) {
      throw new InvalidParameterException(sb.toString());
    }
  }

  protected void waitForAllPS(int psNumber) throws ServiceException, InterruptedException {
    boolean isAllPSReady = true;
    while (true) {
      GetAllPSLocationResponse response =
        master.getAllPSLocation(null, GetAllPSLocationRequest.newBuilder().build());
      List<PSLocationProto> psLocs = response.getPsLocationsList();
      int size = psLocs.size();
      if (size == psNumber) {
        isAllPSReady = true;
        for (int i = 0; i < size; i++) {
          if (psLocs.get(i).getPsStatus() == PSStatus.PS_NOTREADY) {
            isAllPSReady = false;
            break;
          }
        }

        if (isAllPSReady) {
          return;
        }
      }
      Thread.sleep(100);
    }
  }

  @Override public void close() {
    TConnectionManager.deleteAllConnections(true);
    TConnectionManager.shutDown();
  }

  protected abstract void updateMaster(int maxWaitTimeInSec) throws Exception;

  protected abstract String getAppId();
}
