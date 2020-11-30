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


package com.tencent.angel.master.matrix.committer;

import com.tencent.angel.conf.AngelConf;
import com.tencent.angel.conf.MatrixConf;
import com.tencent.angel.master.app.AMContext;
import com.tencent.angel.master.matrixmeta.AMMatrixMetaManager;
import com.tencent.angel.ml.matrix.MatrixMeta;
import com.tencent.angel.ml.matrix.PartitionMeta;
import com.tencent.angel.model.MatrixSaveContext;
import com.tencent.angel.model.ModelSaveContext;
import com.tencent.angel.model.ModelSaveResult;
import com.tencent.angel.model.ModelSaveRunningContext;
import com.tencent.angel.model.PSMatricesSaveContext;
import com.tencent.angel.model.PSMatricesSaveResult;
import com.tencent.angel.model.PSMatrixSaveContext;
import com.tencent.angel.model.SaveState;
import com.tencent.angel.model.SaveTriggerMode;
import com.tencent.angel.model.io.IOExecutors;
import com.tencent.angel.model.output.format.MatrixFilesMeta;
import com.tencent.angel.model.output.format.ModelFilesConstent;
import com.tencent.angel.model.output.format.PSMatrixFilesMeta;
import com.tencent.angel.ps.ParameterServerId;
import com.tencent.angel.utils.HdfsUtil;
import com.tencent.angel.utils.StringUtils;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Vector;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.RecursiveAction;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.service.AbstractService;

/**
 * Model save manager
 */
public class AMModelSaver extends AbstractService {

  private static final Log LOG = LogFactory.getLog(AMModelSaver.class);
  private final Lock lock;

  /**
   * master context
   */
  private final AMContext context;

  /**
   * Model save contexts
   */
  private final Map<Integer, ModelSaveContext> saveContexts;

  /**
   * Save request id to result map
   */
  private final Map<Integer, ModelSaveResult> results;

  /**
   * Current saving request id
   */
  private int currentRequestId = -1;

  /**
   * PS id to PS sub save request context map
   */
  private Map<ParameterServerId, PSMatricesSaveContext> currentSubSaveContexts;

  /**
   * PS id to sub result map
   */
  private Map<ParameterServerId, PSMatricesSaveResult> subResults;

  /**
   * the dispatcher of combine tasks
   */
  private Thread combineDispatchThread;

  /**
   * Is stop the dispatcher and commit tasks
   */
  private final AtomicBoolean stopped;

  /**
   * HDFS operation executor
   */
  private IOExecutors fileOpExecutor;

  private int saveRequestIdGen = 0;

  /**
   * Enable epoch trigger save model
   */
  private final boolean epochTrigSave;

  /**
   * How many epochs save the model once
   */
  private final int saveModelFrequency;

  /**
   * Waiting save request
   */
  private final List<ModelSaveRunningContext> waitingTasks;

  /**
   * Save request dispatcher
   */
  private volatile Thread dispatcher;

  /**
   * Received sub request results number
   */
  private int receivedSubResult;

  /**
   * Matrix id to save results map
   */
  private final ConcurrentHashMap<Integer, List<SaveResult>> matrixIdToSaveResults;

  /**
   * Matrix id to old model load context map
   */
  private final ConcurrentHashMap<Integer, SaveResult> matrixIdToLoadPathResult;

  /**
   * Matrix id to checkpoint results map
   */
  private final ConcurrentHashMap<Integer, List<SaveResult>> matrixIdToCheckpointResults;

  /**
   * Maximum save results
   */
  private final int maxSaveItem;

  /**
   * Maximum checkpoint number
   */
  private final int maxCheckpointItem;

  /**
   * Create a AMMatrixCommitter
   *
   * @param context master context
   */
  public AMModelSaver(AMContext context) {
    super("model-saver");
    this.context = context;
    this.stopped = new AtomicBoolean(false);
    this.saveContexts = new ConcurrentHashMap<>();
    this.results = new ConcurrentHashMap<>();
    this.waitingTasks = new ArrayList<>();

    this.matrixIdToSaveResults = new ConcurrentHashMap<>();
    this.matrixIdToLoadPathResult = new ConcurrentHashMap<>();
    this.matrixIdToCheckpointResults = new ConcurrentHashMap<>();

    this.lock = new ReentrantLock();

    epochTrigSave = context.getConf().getBoolean(AngelConf.ANGEL_SAVE_MODEL_EPOCH_TIGGER_ENABLE,
        AngelConf.DEFAULT_ANGEL_SAVE_MODEL_EPOCH_TIGGER_ENABLE);
    saveModelFrequency = context.getConf().getInt(AngelConf.ANGEL_SAVE_MODEL_EVERY_HOWMANY_EPOCHS,
        AngelConf.DEFAULT_ANGEL_SAVE_MODEL_EVERY_HOWMANY_EPOCHS);
    maxSaveItem = context.getConf().getInt(AngelConf.ANGEL_SAVE_MODEL_MAX_RESULTS_FOR_SINGLE_MATRIX,
        AngelConf.DEFAULT_ANGEL_SAVE_MODEL_MAX_RESULTS_FOR_SINGLE_MATRIX);
    maxCheckpointItem = context.getConf().getInt(AngelConf.ANGEL_CHECKPOINT_MAX_RESULTS_FOR_SINGLE_MATRIX,
        AngelConf.DEFAULT_ANGEL_CHECKPOINT_MAX_RESULTS_FOR_SINGLE_MATRIX);
  }

  @Override
  public void serviceStart() throws Exception {
    dispatcher = new Thread(new Runnable() {
      @Override
      public void run() {
        while (!stopped.get() && !Thread.interrupted()) {
          try {
            lock.lock();
            if (currentRequestId == -1 && waitingTasks.size() > 0) {
              ModelSaveRunningContext runningContext = waitingTasks.remove(0);
              save(runningContext);
            }
          } finally {
            lock.unlock();
          }
        }

        try {
          Thread.sleep(100);
        } catch (InterruptedException e) {
          if (!stopped.get()) {
            LOG.error("model save dispatcher is interrupt ", e);
          }
        }
      }
    });
    dispatcher.setName("model save dispatcher");
    dispatcher.start();
  }

  @Override
  protected void serviceStop() throws Exception {
    if (stopped.getAndSet(true)) {
      return;
    }

    if (fileOpExecutor != null) {
      fileOpExecutor.shutdown();
    }

    if (combineDispatchThread != null) {
      combineDispatchThread.interrupt();
    }

    if (dispatcher != null) {
      dispatcher.interrupt();
    }

    super.serviceStop();
    LOG.info("Model saver stopped");
  }

  /**
   * Is a saving operation executing now
   *
   * @return true or false
   */
  public boolean isSaving() {
    try {
      lock.lock();
      return currentRequestId > 0;
    } finally {
      lock.unlock();
    }
  }

  /**
   * Model save trigger
   *
   * @param epochIndex current epoch index
   */
  public void epochUpdate(int epochIndex) {
    if (epochTrigSave && (epochIndex > 0) && (epochIndex % saveModelFrequency == 0)) {
      LOG.info("Epoch " + epochIndex + " over, start to save model");
      Map<Integer, MatrixMeta> metas = context.getMatrixMetaManager().getMatrixMetas();
      if (metas.isEmpty()) {
        LOG.info("There are no matrices need save, just return");
        return;
      }

      String finalPath = context.getConf().get(AngelConf.ANGEL_JOB_OUTPUT_PATH);
      ModelSaveContext saveContext = new ModelSaveContext(finalPath);
      for (MatrixMeta meta : metas.values()) {
        String savePath = meta.getMatrixContext().getAttributes().get(MatrixConf.MATRIX_SAVE_PATH);
        if (savePath != null) {
          saveContext.addMatrix(new MatrixSaveContext(meta.getName()));
        }
      }

      try {
        save(saveContext, SaveTriggerMode.EPOCH_TRIGGER);
      } catch (Throwable x) {
        LOG.error("save model failed for epoch " + epochIndex, x);
      }
    }
  }

  /**
   * Save model
   *
   * @param saveContext save model context
   * @return save request id
   */
  public int save(ModelSaveContext saveContext) {
    return save(saveContext, SaveTriggerMode.USER_TRIGGER);
  }

  /**
   * Save model
   *
   * @param saveContext save model context
   * @return save request id
   */
  public int save(ModelSaveContext saveContext, SaveTriggerMode triggerMode) {
    try {
      lock.lock();
      int requestId = saveRequestIdGen++;
      saveContext.setTmpSavePath(HdfsUtil.generateTmpDirectory(context.getConf(),
          context.getApplicationId().toString(), new Path(saveContext.getSavePath())).toString());
      //Path tmpPath = new Path(new Path(context.getConf().get(AngelConf.ANGEL_JOB_TMP_OUTPUT_PATH)),
      //  String.valueOf(requestId));
      //Path tmpPath = HdfsUtil.toTmpPath(new Path(saveContext.getSavePath()));
      //saveContext.setTmpSavePath(tmpPath.toString());

      saveContexts.put(requestId, saveContext);
      results.put(requestId, new ModelSaveResult(requestId));
      results.get(requestId).setState(SaveState.INIT);
      boolean needAdd = true;

      // Filter old epoch trigger first
      if (triggerMode == SaveTriggerMode.EPOCH_TRIGGER) {
        int size = waitingTasks.size();
        for (int i = 0; i < size; i++) {
          if (waitingTasks.get(i).getTriggerMode() == SaveTriggerMode.EPOCH_TRIGGER) {
            LOG.info("there is another epoch trigger model save request waiting, just exit");
            needAdd = false;
            break;
          }
        }
      }
      if (needAdd) {
        waitingTasks.add(new ModelSaveRunningContext(requestId, triggerMode, saveContext));
      }
      return requestId;
    } finally {
      lock.unlock();
    }
  }

  private void save(ModelSaveRunningContext runningContext) {
    ModelSaveContext saveContext = runningContext.getSaveContext();
    try {
      lock.lock();
      currentRequestId = runningContext.getRequestId();
      LOG.info("Start to execute save request " + saveContext + " with request id=" + runningContext
          .getRequestId());

      // Split the user request to sub-requests to pss
      currentSubSaveContexts = split(currentRequestId, saveContext);
      subResults = new HashMap<>(currentSubSaveContexts.size());
      for (Map.Entry<ParameterServerId, PSMatricesSaveContext> entry : currentSubSaveContexts
          .entrySet()) {
        subResults.put(entry.getKey(), new PSMatricesSaveResult(entry.getValue().getRequestId(),
            entry.getValue().getSubRequestId(), SaveState.INIT));
      }
      receivedSubResult = 0;
    } finally {
      lock.unlock();
    }
  }

  /**
   * PS start saving
   *
   * @param psId PS id
   * @param requestId request id
   * @param subRequestId sub-request id
   */
  public void psSaveStart(ParameterServerId psId, int requestId, int subRequestId) {
    try {
      lock.lock();
      if (currentRequestId == requestId) {
        PSMatricesSaveResult subResult = subResults.get(psId);
        subResult.setState(SaveState.SAVING);
      }
    } finally {
      lock.unlock();
    }
  }

  /**
   * PS finish save request
   *
   * @param psId parameter server id
   * @param subResult the result of sub save request
   */
  public void psSaveFinish(ParameterServerId psId, PSMatricesSaveResult subResult) {
    try {
      lock.lock();
      if (subResults == null || subResult.getRequestId() != currentRequestId) {
        return;
      }
      receivedSubResult++;
      subResults.put(psId, subResult);
      LOG.info(
          "save subrequest, complete number=" + receivedSubResult + ", total number=" + subResults
              .size());
      if (receivedSubResult >= subResults.size()) {
        ModelSaveResult result = results.get(subResult.getRequestId());
        if (canCombine()) {
          ModelSaveContext saveContext = saveContexts.get(subResult.getRequestId());
          try {
            result.setState(SaveState.COMBINING);
            combine(saveContext, result);
          } catch (Throwable e) {
            LOG.error("Master combine model files failed ", e);
            saveFailed(result, StringUtils.stringifyException(e));
          }
        } else {
          String failedMsg = combineFailedLogs();
          LOG.error("PS save model failed. " + failedMsg);
          saveFailed(result, failedMsg);
        }
      }
    } finally {
      lock.unlock();
    }
  }

  private String combineFailedLogs() {
    StringBuilder sb = new StringBuilder();
    sb.append("Detail failed log:").append("\n");
    for (Map.Entry<ParameterServerId, PSMatricesSaveResult> entry : subResults.entrySet()) {
      if (entry.getValue().getState() == SaveState.FAILED) {
        sb.append(entry.getKey()).append(":").append(entry.getValue().getErrorMsg()).append("\n");
      }
    }
    return sb.toString();
  }

  /**
   * Get save result
   *
   * @param requestId request id
   * @return save result
   */
  public ModelSaveResult getModelSaveResult(int requestId) {
    try {
      lock.lock();
      return results.get(requestId);
    } finally {
      lock.unlock();
    }
  }

  /**
   * Get save request for a ps
   *
   * @param psId ps id
   * @return the save request for the ps
   */
  public PSMatricesSaveContext getSaveContext(ParameterServerId psId) {
    try {
      lock.lock();
      if (currentRequestId == -1) {
        return null;
      }
      return currentSubSaveContexts.get(psId);
    } finally {
      lock.unlock();
    }
  }

  /**
   * Get save result for a ps
   *
   * @param psId ps id
   * @return the save result for the ps
   */
  public PSMatricesSaveResult getSaveResult(ParameterServerId psId) {
    try {
      lock.lock();
      if (currentRequestId == -1) {
        return null;
      }
      return subResults.get(psId);
    } finally {
      lock.unlock();
    }
  }

  private boolean canCombine() {
    boolean can = true;
    for (PSMatricesSaveResult subResult : subResults.values()) {
      can = can && (subResult.getState() == SaveState.SUCCESS);
    }
    return can;
  }

  private Map<ParameterServerId, PSMatricesSaveContext> split(int requestId,
      ModelSaveContext saveContext) {
    List<MatrixSaveContext> matricesContext = saveContext.getMatricesContext();
    Map<ParameterServerId, List<PSMatrixSaveContext>> psIdToContextsMap = new HashMap<>();
    int size = matricesContext.size();
    for (int i = 0; i < size; i++) {
      Map<ParameterServerId, PSMatrixSaveContext> psIdToContextMap = split(matricesContext.get(i));
      for (Map.Entry<ParameterServerId, PSMatrixSaveContext> matrixEntry : psIdToContextMap
          .entrySet()) {
        List<PSMatrixSaveContext> contexts = psIdToContextsMap.get(matrixEntry.getKey());
        if (contexts == null) {
          contexts = new ArrayList<>();
          psIdToContextsMap.put(matrixEntry.getKey(), contexts);
        }
        contexts.add(matrixEntry.getValue());
      }
    }

    Map<ParameterServerId, PSMatricesSaveContext> ret = new HashMap<>(psIdToContextsMap.size());
    int subRequestId = 0;
    for (Map.Entry<ParameterServerId, List<PSMatrixSaveContext>> modelEntry : psIdToContextsMap
        .entrySet()) {
      Path psPath =
          new Path(
              new Path(new Path(saveContext.getTmpSavePath()), ModelFilesConstent.resultDirName),
              modelEntry.getKey().toString());

      List<PSMatrixSaveContext> psMatrixContexts = modelEntry.getValue();
      for (PSMatrixSaveContext matrixContext : psMatrixContexts) {
        matrixContext.setSavePath(new Path(psPath,
            context.getMatrixMetaManager().getMatrix(matrixContext.getMatrixId()).getName())
            .toString());
      }

      ret.put(modelEntry.getKey(),
          new PSMatricesSaveContext(requestId, subRequestId++, modelEntry.getValue()));
    }
    return ret;
  }

  private Map<ParameterServerId, PSMatrixSaveContext> split(MatrixSaveContext matrixSaveContext) {
    AMMatrixMetaManager matrixMetaManager = context.getMatrixMetaManager();
    MatrixMeta meta = matrixMetaManager.getMatrix(matrixSaveContext.getMatrixName());
    if (meta == null) {
      throw new IllegalStateException("Can not find matrix " + matrixSaveContext.getMatrixName());
    }

    Map<Integer, PartitionMeta> partitions = meta.getPartitionMetas();
    List<Integer> rowIndexes = matrixSaveContext.getRowIndexes();
    Map<ParameterServerId, Set<Integer>> psIdToPartIdsMap = new HashMap<>();
    if (rowIndexes == null || rowIndexes.isEmpty()) {
      for (Map.Entry<Integer, PartitionMeta> partEntry : partitions.entrySet()) {
        ParameterServerId psId = partEntry.getValue().getMasterPs();
        if (psId == null) {
          throw new IllegalStateException("Can not get ps for partition " + partEntry.getKey());
        }
        Set partIds = psIdToPartIdsMap.get(psId);
        if (partIds == null) {
          partIds = new HashSet();
          psIdToPartIdsMap.put(psId, partIds);
        }
        partIds.add(partEntry.getKey());
      }
    } else {
      int size = rowIndexes.size();
      for (int i = 0; i < size; i++) {
        for (Map.Entry<Integer, PartitionMeta> partEntry : partitions.entrySet()) {
          if (!partEntry.getValue().contain(rowIndexes.get(i))) {
            continue;
          }
          ParameterServerId psId = partEntry.getValue().getMasterPs();
          if (psId == null) {
            throw new IllegalStateException("Can not get ps for partition " + partEntry.getKey());
          }
          Set partIds = psIdToPartIdsMap.get(psId);
          if (partIds == null) {
            partIds = new HashSet();
            psIdToPartIdsMap.put(psId, partIds);
          }
          partIds.add(partEntry.getKey());
        }
      }
    }

    int matrixId = meta.getId();
    Map<ParameterServerId, PSMatrixSaveContext> ret = new HashMap<>(psIdToPartIdsMap.size());
    for (Map.Entry<ParameterServerId, Set<Integer>> entry : psIdToPartIdsMap.entrySet()) {
      List<Integer> partIds = new ArrayList<>(entry.getValue());
      partIds.sort(new Comparator<Integer>() {
        @Override
        public int compare(Integer id1, Integer id2) {
          return id1 - id2;
        }
      });
      PSMatrixSaveContext psMatrixSaveContext =
          new PSMatrixSaveContext(matrixId, partIds, matrixSaveContext.getRowIndexes(),
              matrixSaveContext.getFormatClassName(), null, false, true);
      ret.put(entry.getKey(), psMatrixSaveContext);
    }
    return ret;
  }

  public List<SaveResult> getSaveResults(int matrixId) {
    SaveResult loadPathResult = null;
    try {
      loadPathResult = getLoadPath(matrixId);
    } catch (IOException e) {
      LOG.warn("Get load path result failed ", e);
    }

    if(loadPathResult == null) {
      return matrixIdToSaveResults.get(matrixId);
    } else {
      List<SaveResult> results = new ArrayList<>();
      results.add(loadPathResult);
      if(matrixIdToSaveResults.get(matrixId) != null) {
        results.addAll(matrixIdToSaveResults.get(matrixId));
      }
      return results;
    }
  }

  public List<SaveResult> getCheckpointResults(int matrixId) {
    return matrixIdToCheckpointResults.get(matrixId);
  }

  private SaveResult getLoadPath(int matrixId) throws IOException {
    SaveResult result = matrixIdToLoadPathResult.get(matrixId);
    if(result == null) {
      String loadPath = context.getConf().get(AngelConf.ANGEL_LOAD_MODEL_PATH);
      if(loadPath != null) {
        AMMatrixMetaManager matrixMetaManager = context.getMatrixMetaManager();
        MatrixMeta meta = matrixMetaManager.getMatrix(matrixId);
        if(meta != null) {
          Path matrixPath = new Path(loadPath, meta.getName());
          FileSystem fs = matrixPath.getFileSystem(context.getConf());
          if(fs.exists(matrixPath)) {
            result = new SaveResult(loadPath, matrixPath.toString(), -1);
            matrixIdToLoadPathResult.putIfAbsent(matrixId, result);
          }
        }
      }
    }
    return result;
  }

  /**
   * Matrices commit operator
   */
  class ModelCombineOp extends RecursiveAction {

    private final ModelSaveContext saveContext;
    private final Vector<String> errorLogs;
    private final int startPos;
    private final int endPos;
    private final Path tmpCombinePath;
    private final FileSystem fs;

    public ModelCombineOp(ModelSaveContext saveContext, Vector<String> errorLogs, int startPos,
        int endPos, Path tmpCombinePath, FileSystem fs) {
      this.saveContext = saveContext;
      this.errorLogs = errorLogs;
      this.startPos = startPos;
      this.endPos = endPos;
      this.tmpCombinePath = tmpCombinePath;
      this.fs = fs;
    }

    @Override
    protected void compute() {
      if (endPos <= startPos) {
        return;
      }

      if (endPos - startPos == 1) {
        try {
          combineMatrix(saveContext, saveContext.getMatricesContext().get(startPos), errorLogs,
              tmpCombinePath, fs);
        } catch (Throwable e) {
          String matrixName = saveContext.getMatricesContext().get(startPos).getMatrixName();
          errorLogs.add("merge output files for matrix " + matrixName + " failed, error log is " + e
              .getMessage());
          LOG.error("merge output files for matrix " + matrixName + " failed. ", e);
        }
      } else {
        int middle = (startPos + endPos) / 2;
        ModelCombineOp opLeft =
            new ModelCombineOp(saveContext, errorLogs, startPos, middle, tmpCombinePath, fs);
        ModelCombineOp opRight =
            new ModelCombineOp(saveContext, errorLogs, middle, endPos, tmpCombinePath, fs);
        invokeAll(opLeft, opRight);
      }
    }
  }

  /**
   * Combine all output files of a model to a combine directory
   *
   * @param matrixContext matrix save context
   * @param errorLogs error logs
   */
  private void combineMatrix(ModelSaveContext saveContext, MatrixSaveContext matrixContext,
      Vector<String> errorLogs, Path tmpCombinePath, FileSystem fs) {
    LOG.info("start commit matrix " + matrixContext.getMatrixName());

    // Init matrix files meta
    int matrixId = context.getMatrixMetaManager().getMatrix(matrixContext.getMatrixName()).getId();
    List<ParameterServerId> psIds =
        new ArrayList<>(context.getMatrixMetaManager().getMasterPsIds(matrixId));
    MatrixMeta meta = context.getMatrixMetaManager().getMatrix(matrixId);
    Map<String, String> kvMap = meta.getAttributes();

    MatrixFilesMeta filesMeta =
        new MatrixFilesMeta(matrixId, meta.getName(), matrixContext.getFormatClassName(),
            meta.getRowType().getNumber(), meta.getRowNum(),
            meta.getColNum(), meta.getBlockRowNum(), meta.getBlockColNum(), kvMap);

    filesMeta.setFeatureIndexStart(meta.getIndexStart());
    filesMeta.setFeatureIndexEnd(meta.getIndexEnd());

    try {
      // Move output files
      Path srcPath = new Path(saveContext.getTmpSavePath(), ModelFilesConstent.resultDirName);
      Path destPath = new Path(tmpCombinePath, meta.getName());
      PSModelCombineOp partCombineOp =
          new PSModelCombineOp(srcPath, destPath, psIds, errorLogs, filesMeta, 0, psIds.size(), fs);
      fileOpExecutor.execute(partCombineOp);
      partCombineOp.join();

      // Write the meta file
      long startTs = System.currentTimeMillis();
      Path metaFile = new Path(destPath, ModelFilesConstent.modelMetaFileName);
      Path tmpMetaFile = HdfsUtil.toTmpPath(metaFile);
      FSDataOutputStream metaOut = fs.create(tmpMetaFile);
      filesMeta.write(metaOut);
      metaOut.flush();
      metaOut.close();
      HdfsUtil.rename(tmpMetaFile, metaFile, fs);
      LOG.info("commit meta file use time=" + (System.currentTimeMillis() - startTs));
    } catch (Throwable x) {
      errorLogs.add("move output files for matrix " + meta.getName() + " failed, error msg = " + x
          .getMessage());
      LOG.error("move output files for matrix " + meta.getName() + " failed.", x);
    }
  }


  /**
   * Model partitions committer
   */
  class PSModelCombineOp extends RecursiveAction {

    private final Path moveSrcPath;
    private final Path moveDestPath;
    private final List<ParameterServerId> psList;
    private final Vector<String> errorLogs;
    private final MatrixFilesMeta matrixMeta;
    private final int startPos;
    private final int endPos;
    private final FileSystem fs;

    public PSModelCombineOp(Path moveSrcPath, Path moveDestPath, List<ParameterServerId> psList,
        Vector<String> errorLogs, MatrixFilesMeta matrixMeta, int startPos, int endPos,
        FileSystem fs) {
      this.moveSrcPath = moveSrcPath;
      this.moveDestPath = moveDestPath;
      this.psList = psList;
      this.errorLogs = errorLogs;
      this.matrixMeta = matrixMeta;
      this.startPos = startPos;
      this.endPos = endPos;
      this.fs = fs;
    }

    @Override
    protected void compute() {
      if (endPos <= startPos) {
        return;
      }

      if (endPos - startPos == 1) {
        combinePartitions(moveSrcPath, moveDestPath, psList.get(startPos), errorLogs, matrixMeta,
            fs);
      } else {
        int middle = (startPos + endPos) / 2;
        PSModelCombineOp opLeft =
            new PSModelCombineOp(moveSrcPath, moveDestPath, psList, errorLogs, matrixMeta, startPos,
                middle, fs);
        PSModelCombineOp opRight =
            new PSModelCombineOp(moveSrcPath, moveDestPath, psList, errorLogs, matrixMeta, middle,
                endPos, fs);
        invokeAll(opLeft, opRight);
      }
    }
  }


  /**
   * Move all model output files generated by a PS to the combine directory
   *
   * @param moveSrcPath source path
   * @param moveDestPath dest path
   * @param psId parameter server id
   * @param errorLogs error logs
   * @param matrixMeta model files meta
   */
  private void combinePartitions(Path moveSrcPath, Path moveDestPath, ParameterServerId psId,
      Vector<String> errorLogs, MatrixFilesMeta matrixMeta, FileSystem fs) {
    Path psPath = new Path(moveSrcPath, String.valueOf(psId));
    Path serverMatrixPath = new Path(psPath, matrixMeta.getMatrixName());

    Path psMetaFilePath = new Path(serverMatrixPath, ModelFilesConstent.psModelMetaFileName);

    try {
      FSDataInputStream input = fs.open(psMetaFilePath);
      PSMatrixFilesMeta serverMatrixMeta = new PSMatrixFilesMeta();
      serverMatrixMeta.read(input);
      input.close();
      fs.delete(psMetaFilePath, false);

      matrixMeta.merge(serverMatrixMeta);
      HdfsUtil.copyFilesInSameHdfs(serverMatrixPath, moveDestPath, fs);
      LOG.info(
          "copy files of matrix " + matrixMeta.getMatrixName() + " from " + serverMatrixPath
              + " to "
              + moveDestPath + " success.");
    } catch (Throwable x) {
      errorLogs.add(
          "copy files of matrix " + matrixMeta.getMatrixName() + " from " + serverMatrixPath
              + " to "
              + moveDestPath + " failed, error log is " + x.getMessage());
      LOG.error(
          "copy files of matrix " + matrixMeta.getMatrixName() + " from " + serverMatrixPath
              + " to "
              + moveDestPath + " failed. ", x);
    }
  }

  private void combine(ModelSaveContext saveContext, ModelSaveResult result) throws IOException {
    Path tmpPath = new Path(saveContext.getTmpSavePath());
    final FileSystem fs = tmpPath.getFileSystem(context.getConf());
    final Path tmpCombinePath = HdfsUtil.toFinalPath(tmpPath);
    if (fs.exists(tmpCombinePath)) {
      fs.delete(tmpCombinePath, true);
    }

    combineDispatchThread = new Thread(new Runnable() {
      @Override
      public void run() {
        Vector<String> errorLogs = new Vector<>();
        ModelCombineOp op =
            new ModelCombineOp(saveContext, errorLogs, 0, saveContext.getMatricesContext().size(),
                tmpCombinePath, fs);
        fileOpExecutor = new IOExecutors(context.getConf()
            .getInt(AngelConf.ANGEL_AM_MATRIX_DISKIO_WORKER_POOL_SIZE,
                AngelConf.DEFAULT_ANGEL_AM_MATRIX_DISKIO_WORKER_POOL_SIZE));
        fileOpExecutor.init();
        fileOpExecutor.start();

        try {
          fileOpExecutor.execute(op);
          op.join();
          if (!errorLogs.isEmpty()) {
            String errorLog = "move output files for matrice failed, error msg = " + StringUtils
                .join(";", errorLogs);
            LOG.error(errorLog);
            saveFailed(result, errorLog);
          } else {
            finalCommit(tmpCombinePath, new Path(saveContext.getSavePath()), fs);
            saveSuccess(result);
            recordSaveResult(saveContext);
          }
        } catch (Throwable x) {
          LOG.error("move output files for matrice failed. ", x);
          saveFailed(result, StringUtils.stringifyException(x));
        } finally {
          clean(saveContext);
        }
      }
    });

    combineDispatchThread.setName("CommitTaskDispacher");
    combineDispatchThread.start();
  }

  private void recordSaveResult(ModelSaveContext saveContext) {
    List<MatrixSaveContext> matricesContext = saveContext.getMatricesContext();
    for (MatrixSaveContext matrixContext : matricesContext) {
      int matrixId = context.getMatrixMetaManager().getMatrix(matrixContext.getMatrixName())
          .getId();

      List<SaveResult> results;
      if(saveContext.isCheckpoint()) {
        results = matrixIdToCheckpointResults.get(matrixId);
        if (results == null) {
          results = matrixIdToCheckpointResults.putIfAbsent(matrixId, new ArrayList<>());
          if (results == null) {
            results = matrixIdToCheckpointResults.get(matrixId);
          }
        }
      } else {
        results = matrixIdToSaveResults.get(matrixId);
        if (results == null) {
          results = matrixIdToSaveResults.putIfAbsent(matrixId, new ArrayList<>());
          if (results == null) {
            results = matrixIdToSaveResults.get(matrixId);
          }
        }
      }

      results.add(new SaveResult(saveContext.getSavePath(),
          new Path(saveContext.getSavePath(), matrixContext.getMatrixName()).toString(),
          System.currentTimeMillis()));

      LOG.info("Matrix " + matrixContext.getMatrixName() + " new save path = "
          + new Path(saveContext.getSavePath(), matrixContext.getMatrixName()).toString());
      LOG.info("After this save, total save result number=" + results.size());

      int maxSaveNum = saveContext.isCheckpoint() ? maxCheckpointItem : maxSaveItem;
      while (results.size() > maxSaveNum) {
        SaveResult oldResult = results.remove(0);
        LOG.info("need remove old save results/checkpoint for matrix " + matrixContext.getMatrixName() +
            " remove path = " + oldResult.getMatrixPath());
        try {
          HdfsUtil.remove(context.getConf(), oldResult.getMatrixPath());
          HdfsUtil.removeIfEmpty(context.getConf(), oldResult.getModelPath());
        } catch (IOException e) {
          LOG.warn("remove old save result/checkpoint " + saveContext.getSavePath() + " for matrix "
              + matrixContext.getMatrixName() + " failed ");
        }
      }
    }
  }

  private void clean(ModelSaveContext saveContext) {
    Path tmpPath = new Path(saveContext.getTmpSavePath());
    try {
      FileSystem fs = tmpPath.getFileSystem(context.getConf());
      boolean ret = fs.delete(tmpPath, true);
      if (ret) {
        LOG.info("Delete tmp directory " + tmpPath.toString() + " success");
      } else {
        LOG.error("Delete tmp directory " + tmpPath.toString() + " failed");
      }
    } catch (Throwable e) {
      LOG.error("clean tmp directory " + saveContext.getTmpSavePath() + " failed ", e);
    }
  }

  private void saveFailed(ModelSaveResult result, String errorLog) {
    try {
      lock.lock();
      result.setState(SaveState.FAILED);
      result.setMessage(errorLog);
      currentRequestId = -1;
      receivedSubResult = 0;
    } finally {
      lock.unlock();
    }
  }

  private void saveSuccess(ModelSaveResult result) {
    try {
      lock.lock();
      result.setState(SaveState.SUCCESS);
      currentRequestId = -1;
      receivedSubResult = 0;
    } finally {
      lock.unlock();
    }
  }

  private void finalCommit(Path tmpCombinePath, Path outputPath, FileSystem fs) throws IOException {
    HdfsUtil.rename(tmpCombinePath, outputPath, fs);
    LOG.info("final commit from " + tmpCombinePath + " to " + outputPath);
  }
}
