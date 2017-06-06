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

package com.tencent.angel.client;

import com.google.protobuf.ServiceException;
import com.tencent.angel.RunningMode;
import com.tencent.angel.common.Location;
import com.tencent.angel.conf.AngelConfiguration;
import com.tencent.angel.conf.MatrixConfiguration;
import com.tencent.angel.exception.AngelException;
import com.tencent.angel.exception.InvalidParameterException;
import com.tencent.angel.ipc.TConnectionManager;
import com.tencent.angel.master.MasterProtocol;
import com.tencent.angel.ml.matrix.MatrixContext;
import com.tencent.angel.ml.model.MLModel;
import com.tencent.angel.ml.model.PSModel;
import com.tencent.angel.protobuf.RequestConverter;
import com.tencent.angel.protobuf.generated.ClientMasterServiceProtos.*;
import com.tencent.angel.protobuf.generated.MLProtos.GetAllPSLocationRequest;
import com.tencent.angel.protobuf.generated.MLProtos.GetAllPSLocationResponse;
import com.tencent.angel.protobuf.generated.MLProtos.MatrixProto;
import com.tencent.angel.protobuf.generated.MLProtos.MatrixStatus;
import com.tencent.angel.protobuf.generated.MLProtos.PSLocation;
import com.tencent.angel.protobuf.generated.MLProtos.PSStatus;
import com.tencent.angel.protobuf.generated.MLProtos.Pair;
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

import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Angel application client. It provides the control interfaces for the application.
 */
public abstract class AngelClient implements AngelClientInterface {
  private static final Log LOG = LogFactory.getLog(AngelClient.class);

  /** application configuration */
  protected final Configuration conf;

  /** matrices used in the application */
  private final List<MatrixProto> matrixList;

  /** rpc client to master */
  protected MasterProtocol master;

  private GetJobReportRequest getJobReportReq;
  private GetJobReportRequest.Builder getJobReportReqBuilder;
  private GetJobReportResponse lastReport;
  private boolean isExecuteFinished;
  private boolean isFinished;
  private String appFailedMessage;

  /** temporary file use to store application state */
  protected Path internalStateFile;

  /** the application submitting user */
  protected String userName;

  /** master location */
  protected Location masterLocation;
  
  /**
   * 
   * Create a new AngelClient.
   *
   * @param conf application configuration
   */
  public AngelClient(Configuration conf){
    this.conf = conf;
    matrixList = new ArrayList<MatrixProto>();
    isExecuteFinished = false;
    isFinished = false;
  }
  
  @SuppressWarnings("rawtypes")
  @Override
  public void runTask(Class<? extends BaseTask> taskClass) throws AngelException {
    try {
      master.setParams(
          null,
          SetParamsRequest
              .newBuilder()
              .addKvs(
                  Pair.newBuilder().setKey(AngelConfiguration.ANGEL_TASK_USER_TASKCLASS)
                      .setValue(taskClass.getName()).build()).build());
      master.start(null, StartRequest.newBuilder().build());
    } catch (ServiceException e) {
      LOG.error("start application failed.", e);
      throw new AngelException(e);
    }
  }

  @Override
  public void run() throws AngelException {    
    try {
      createMatrices();
      master.start(null, StartRequest.newBuilder().build());
    } catch (ServiceException | InvalidParameterException e) {
      LOG.error("start application failed.", e);
      throw new AngelException(e);
    }
  }
  
  @Override
  public void addMatrix(MatrixContext mContext) throws AngelException {
    try{
      matrixList.add(mContext.buildMatProto(conf));
    } catch (Exception x) {
      throw new AngelException(x);
    }
  }

  @SuppressWarnings("rawtypes")
  @Override
  public void loadModel(MLModel model) throws AngelException {
    Map<String, PSModel<?>> psModels = model.getPsModels();

    for (Map.Entry<String, PSModel<?>> entry: psModels.entrySet()) {
      addMatrix(entry.getValue().getContext());
    }   
    
    try {
      createMatrices();
    } catch (InvalidParameterException | ServiceException e) {
      throw new AngelException("create matrices failed.", e);
    }
  }

  @SuppressWarnings("rawtypes")
  @Override
  public void saveModel(MLModel model) throws AngelException {
    SaveRequest.Builder builder = SaveRequest.newBuilder();
    Map<String, PSModel<?>> psModels = model.getPsModels();

    for (Map.Entry<String, PSModel<?>> entry: psModels.entrySet()) {
      MatrixContext context = entry.getValue().getContext();
      String savePath = context.getAttributes().get(MatrixConfiguration.MATRIX_SAVE_PATH);
      if(savePath != null) {
        builder.addMatrixNames(context.getName());
      }
    }

    try {
      master.save(null, builder.build());
    } catch (ServiceException e) {
      LOG.error("save model failed.", e);
      throw new AngelException(e);
    }
    
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
  }

  /**
   * Save matrices to files.
   * @param matrixNames need save matrix name list
   */
  public void saveMatrices(List<String> matrixNames) {
    SaveRequest.Builder builder = SaveRequest.newBuilder();
    builder.addAllMatrixNames(matrixNames);

    try {
      master.save(null, builder.build());
    } catch (ServiceException e) {
      LOG.error("save model failed.", e);
      throw new AngelException(e);
    }

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
  }

  @Override
  public void stop(int stateCode) throws AngelException {
    stop();
  }

  @Override
  public void waitForCompletion() throws AngelException{
    RunningMode mode = RunningMode.valueOf(conf.get(AngelConfiguration.ANGEL_RUNNING_MODE, AngelConfiguration.DEFAULT_ANGEL_RUNNING_MODE));
    switch (mode) {
      case ANGEL_PS:{
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
      case ANGEL_PS_WORKER:{
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

        String actionType = conf.get(AngelConfiguration.ANGEL_ACTION_TYPE, AngelConfiguration.DEFAULT_ANGEL_ACTION_TYPE);
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
    String outPathStr = conf.get(AngelConfiguration.ANGEL_JOB_OUTPUT_PATH);
    String tmpPathStr = conf.get(AngelConfiguration.ANGEL_JOB_TMP_OUTPUT_DIRECTORY);
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
  private boolean isCompleted(){
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
    if (jobState == JobStateProto.J_SUCCEEDED || jobState == JobStateProto.J_FAILED || jobState == JobStateProto.J_KILLED) {
      isFinished = true;
      LOG.info("job is finished! status: " + jobState);
      if (jobState == JobStateProto.J_FAILED
          || jobState == JobStateProto.J_KILLED) {

        appFailedMessage = " detail is " + lastReport.getJobReport().getDiagnostics();
        LOG.error(appFailedMessage);
      }
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
      if (jobState == JobStateProto.J_FAILED
          || jobState == JobStateProto.J_KILLED) {

        appFailedMessage = " detail is " + lastReport.getJobReport().getDiagnostics();
        LOG.error(appFailedMessage);
      }
      return true;
    } else {
      return false;
    }
  }

  protected void printWorkerLogUrl(WorkerId workerId) {}

  private void updateJobReport() {
    GetJobReportRequest getJobRequest = getGetJobReportRequest();
    GetJobReportResponse response = null;
    try {
      response = master.getJobReport(null, getJobRequest);
    } catch (Exception e) {
      LOG.error("getJobReport from master failed. " + e.getMessage());
      try {
        updateMaster(60);
        if(master != null) {
          response = master.getJobReport(null, getJobRequest);
        }
      } catch (Exception e1) {
        LOG.error("update master failed.", e1);
      }
    }

    if(response == null) {
      isFinished = true;
      lastReport = null;
      return;
    }

    JobReportProto report = response.getJobReport();
    // JobStateProto jobState = report.getJobState();
    if (lastReport == null
        || (report.hasCurIteration() && report.getCurIteration() != lastReport.getJobReport()
            .getCurIteration())) {
      LOG.info("curIteration: " + report.getCurIteration());
      if (report.hasLoss()) {
        LOG.info("loss/success: " + report.getLoss() + "/" + report.getSuccess());
      }
    }
    lastReport = response;
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
  
  protected void createMatrices() throws InvalidParameterException, ServiceException {
    CreateMatricesRequest createMatricsRequest =
        RequestConverter.buildCreateMatricesRequest(matrixList);
    master.createMatrices(null, createMatricsRequest);
    waitForMatricesCreated(matrixList);
  }
  
  private void waitForMatricesCreated(List<MatrixProto> matrixList) throws ServiceException {
    CheckMatricesCreatedRequest.Builder builder = CheckMatricesCreatedRequest.newBuilder();
    int size = matrixList.size();
    for(int i = 0; i < size; i++) {
      builder.addMatrixNames(matrixList.get(i).getName());
    }
    CheckMatricesCreatedRequest request = builder.build();
    
    boolean isAllCreated = true;
    while(true) {
      CheckMatricesCreatedResponse response = master.checkMatricesCreated(null, request);
      List<MatrixStatus> status = response.getStatusList();
      assert(size == status.size());
      
      isAllCreated = true;
      for(int i = 0; i < size; i++) {
        if(status.get(i) != MatrixStatus.M_OK) {
          isAllCreated = false;
          break;
        }
      }
      
      if(isAllCreated) {
        return;
      }
      
      try {
        Thread.sleep(1000);
      } catch (InterruptedException e) {
        LOG.warn("waitForMatricesCreated is interrupted.");
      }
    }
  }

  protected void setOutputDirectory() throws IOException{
    String actionType = conf.get(AngelConfiguration.ANGEL_ACTION_TYPE, AngelConfiguration.DEFAULT_ANGEL_ACTION_TYPE);
    String path = null;
    if (!actionType.matches("predict")) {
      path = conf.get(AngelConfiguration.ANGEL_SAVE_MODEL_PATH);
    } else {
      path = conf.get(AngelConfiguration.ANGEL_PREDICT_PATH);
    }
    
    conf.set(AngelConfiguration.ANGEL_JOB_OUTPUT_PATH, path);

    // generate tmp output directory
    String outputPathStr = conf.get(AngelConfiguration.ANGEL_JOB_OUTPUT_PATH);
    if(outputPathStr == null) {
      throw new IOException("output directory is null. you must set "
        + AngelConfiguration.ANGEL_SAVE_MODEL_PATH + " at training mode or set "
        + AngelConfiguration.ANGEL_PREDICT_PATH + " at predict mode");
    }

    boolean deleteOnExist =
        conf.getBoolean(AngelConfiguration.ANGEL_JOB_OUTPUT_PATH_DELETEONEXIST,
            AngelConfiguration.DEFAULT_ANGEL_JOB_OUTPUT_PATH_DELETEONEXIST);

    Path outputPath = new Path(outputPathStr);

    FileSystem outFs = outputPath.getFileSystem(conf);
    if (outFs.exists(outputPath)) {
      if (deleteOnExist) {
        outFs.delete(outputPath, true);
      } else {
        throw new IOException("output path " + outputPath + " already exist, please check");
      }
    }

    Path tmpOutputPath = HdfsUtil.generateTmpDirectory(conf, getAppId(), outputPath);

    internalStateFile = new Path(HdfsUtil.generateTmpDirectory(conf, getAppId(), outputPath), "state");

    conf.set(AngelConfiguration.ANGEL_JOB_TMP_OUTPUT_DIRECTORY, tmpOutputPath.toString());
    LOG.info(AngelConfiguration.ANGEL_JOB_TMP_OUTPUT_DIRECTORY + "=" + tmpOutputPath.toString());

    LOG.info("internal state file is " + internalStateFile);
    conf.set(AngelConfiguration.ANGEL_APP_SERILIZE_STATE_FILE, internalStateFile.toString());
    
  }
  
  protected void setUser() throws ClassNotFoundException, NoSuchFieldException, SecurityException, InstantiationException, IllegalAccessException, IOException{
    userName = UGITools.getCurrentUser(conf).getShortUserName();
    conf.set(AngelConfiguration.USER_NAME, userName);
  }

  abstract public void startPSServer() throws AngelException;

  protected void setLocalAddr() throws UnknownHostException {
    InetAddress ip = InetAddress.getLocalHost();
    if (ip != null) {
      String submitHostAddress = ip.getHostAddress();
      String submitHostName = ip.getHostName();
      conf.set(AngelConfiguration.JOB_SUBMITHOST, submitHostName);
      conf.set(AngelConfiguration.JOB_SUBMITHOSTADDR, submitHostAddress);
    }
  }
  
  protected void checkParameters(Configuration conf) throws InvalidParameterException {
    StringBuilder sb = new StringBuilder();
    int coreNum = conf.getInt(AngelConfiguration.ANGEL_AM_CPU_VCORES,
        AngelConfiguration.DEFAULT_ANGEL_AM_CPU_VCORES);
    if (coreNum <= 0) {
      sb.append(AngelConfiguration.ANGEL_AM_CPU_VCORES + " should > 0");
      sb.append("\n");
    }

    int memNum = conf.getInt(AngelConfiguration.ANGEL_AM_VMEM_MB,
        AngelConfiguration.DEFAULT_ANGEL_AM_VMEM_MB);
    if (memNum <= 0) {
      sb.append(AngelConfiguration.ANGEL_AM_VMEM_MB + " should > 0");
      sb.append("\n");
    }

    int heartbeatInterval = conf.getInt(
        AngelConfiguration.ANGEL_AM_HEARTBEAT_INTERVAL_MS,
        AngelConfiguration.DEFAULT_ANGEL_AM_HEARTBEAT_INTERVAL_MS);

    if (heartbeatInterval <= 0) {
      sb.append(AngelConfiguration.ANGEL_AM_HEARTBEAT_INTERVAL_MS
          + " should > 0");
      sb.append("\n");
    }

    int commitTaskNum = conf.getInt(
        AngelConfiguration.ANGEL_AM_COMMIT_TASK_NUM,
        AngelConfiguration.DEFAULT_ANGEL_AM_COMMIT_TASK_NUM);
    if (commitTaskNum <= 0 || commitTaskNum > 1024) {
      sb.append(AngelConfiguration.ANGEL_AM_COMMIT_TASK_NUM
          + " should > 0 and <= 1024");
      sb.append("\n");
    }

    int amCommitTimeOutMS = conf.getInt(
        AngelConfiguration.ANGEL_AM_COMMIT_TIMEOUT_MS,
        AngelConfiguration.DEFAULT_ANGEL_AM_COMMIT_TIMEOUT_MS);
    if (amCommitTimeOutMS <= 0) {
      sb.append(AngelConfiguration.ANGEL_AM_COMMIT_TIMEOUT_MS + " should > 0");
      sb.append("\n");
    }

    int containerLauncherLimit = conf
        .getInt(
            AngelConfiguration.ANGEL_AM_CONTAINERLAUNCHER_THREAD_COUNT_LIMIT,
            AngelConfiguration.DEFAULT_ANGEL_AM_CONTAINERLAUNCHER_THREAD_COUNT_LIMIT);
    if (containerLauncherLimit <= 0) {
      sb.append(AngelConfiguration.ANGEL_AM_CONTAINERLAUNCHER_THREAD_COUNT_LIMIT
          + " should > 0");
      sb.append("\n");
    }

    if(sb.length() > 0) {
      throw new InvalidParameterException(sb.toString());
    }
  }
  
  protected void waitForAllPS(int psNumber) throws ServiceException, InterruptedException {
    boolean isAllPSReady = true;
    while(true) {
      GetAllPSLocationResponse response = master.getAllPSLocation(null, GetAllPSLocationRequest.newBuilder().build());
      List<PSLocation> psLocs = response.getPsLocationsList();
      int size = psLocs.size();
      if(size == psNumber) {
        isAllPSReady = true;
        for(int i = 0; i < size; i++) {
          if(psLocs.get(i).getPsStatus() == PSStatus.PS_NOTREADY) {
            isAllPSReady = false;
            break;
          }
        }
        
        if(isAllPSReady) {
          return;
        }
      }       
      Thread.sleep(100);
    }
  }

  protected void close() {
    TConnectionManager.deleteAllConnections(true);
    TConnectionManager.shutDown();
  }
  
  protected abstract void updateMaster(int maxWaitTimeInSec) throws Exception;
  protected abstract String getAppId();
}
