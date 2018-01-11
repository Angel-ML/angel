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

package com.tencent.angel.master.oplog;

import com.tencent.angel.conf.AngelConf;
import com.tencent.angel.exception.InvalidParameterException;
import com.tencent.angel.master.app.AMContext;
import com.tencent.angel.master.data.DataSpliter;
import com.tencent.angel.master.matrixmeta.AMMatrixMetaManager;
import com.tencent.angel.master.ps.ParameterServerManager;
import com.tencent.angel.master.ps.ps.AMParameterServer;
import com.tencent.angel.master.task.AMTaskManager;
import com.tencent.angel.ps.PSAttemptId;
import com.tencent.angel.ps.ParameterServerId;
import com.tencent.angel.utils.HdfsUtil;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.service.AbstractService;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Application state storage. It write the application state to files every fixed period of time
 */
public class AppStateStorage extends AbstractService {
  private static final Log LOG = LogFactory.getLog(AppStateStorage.class);

  private static final String matrixMetaFileNamePrefix = "matrixmeta";
  private static final String fileNameSeparator = "_";
  private static final String splitFileName = "splits";
  private static final String psMetaFileNamePrefix = "psmeta";
  private static final String taskMetaFileNamePrefix = "taskmeta";
  private static final String tmpPrefix = "_tmp.";
  
  private final AMContext context;
  
  /**ps meta and task meta write thread*/
  private Thread writter;

  /**base state file directory*/
  private final String writeDir;
  private final Path writeDirPath;
  
  /**ps meta and task meta write interval in milliseconds*/
  private final int writeIntervalMS;
  
  /**whether you want to stop the application state write service*/
  private final AtomicBoolean stopped;
  private final FileSystem fs;
  
  /**last ps meta file path*/
  private Path lastPsMetaFilePath;
  
  /**last task meta file path*/
  private Path lasttaskMetaFilePath;
  
  /**data splits file path*/
  private final Path splitFilePath;
  
  /**last matrix meta file path*/
  private Path lastMatrixMetaFilePath;  
  
  private final Lock matrixMetaLock;
  private final Lock taskMetaLock;
  private final Lock psMetaLock;

  /**
   * Create a AppStateStorage
   * @param context master context
   * @param writeDir storage file directory
   * @param fs 
   */
  public AppStateStorage(AMContext context, String writeDir, FileSystem fs){
    super("app-state-writter");
    LOG.info("writeDir=" + writeDir);
    this.context = context;
    this.writeDir = writeDir;
    this.writeDirPath = new Path(writeDir);
    this.fs = fs;
    
    splitFilePath = new Path(writeDirPath, splitFileName);
    matrixMetaLock = new ReentrantLock();
    taskMetaLock = new ReentrantLock();
    psMetaLock = new ReentrantLock();
    
    writeIntervalMS = context.getConf().getInt(
        AngelConf.ANGEL_AM_WRITE_STATE_INTERVAL_MS,
        AngelConf.DEFAULT_ANGEL_AM_WRITE_STATE_INTERVAL_MS);
    this.stopped = new AtomicBoolean(false);
  }

  @Override
  protected void serviceStart() throws Exception {
    startWriter();
    super.serviceStart();
  }

  private void startWriter() {
    writter = new Writter();
    writter.setName("app-state-writter");
    writter.start();
  }

  @Override
  protected void serviceInit(Configuration conf) throws Exception {
    super.serviceInit(conf);
  }

  @Override
  protected void serviceStop() throws Exception {
    if (stopped.getAndSet(true)) {
      return;
    }
    if (writter != null) {
      writter.interrupt();
      try {
        writter.join();
      } catch (InterruptedException ie) {
        LOG.warn("app-state-writter InterruptedException while stopping");
      }
    }

    LOG.info("app-state-writter service stop!");
  }
  
  /**
   * write training data split meta data to file
   * @param dataSpliter training data split meta storage
   * 
   */
  public void writeDataSplits(DataSpliter dataSpliter) throws IOException {
    //write meta data to a temporary file
    String tmpFile = getTmpDataSplitsFile(); 
    Path tmpPath = new Path(writeDir, tmpFile);
    FSDataOutputStream outputStream = fs.create(tmpPath);
    dataSpliter.serialize(outputStream);
    outputStream.close();
    
    //rename the temporary file to final file
    HdfsUtil.rename(tmpPath, splitFilePath, fs);
  }
  
  private String getTmpDataSplitsFile(){
    return tmpPrefix + splitFileName;
  }
  
  /**
   * check if the training data meta file exists
   * @return true if exists, otherwise false
   */
  public boolean checkSplitFileExist() throws IOException{
    return fs.exists(new Path(writeDir, splitFileName));
  }
  
  /**
   * load training data meta information from file
   * @return DataSpliter training data split meta storage, if the file does not exist, just return null
   */
  public DataSpliter loadDataSplits() throws IOException, ClassNotFoundException {
    //if the file exists, read from file and deserialize it
    if(fs.exists(splitFilePath)) {
      FSDataInputStream inputStream = fs.open(new Path(writeDir, splitFileName));
      DataSpliter dataSpliter = new DataSpliter(context);
      dataSpliter.deserialize(inputStream);
      return dataSpliter;
    } else {
      LOG.warn("data split file does not exist");
      return null;
    }
  }
  
  /**
   * write matrix meta to file
   * @param matrixMetaManager matrix meta storage
   * @throws IOException
   */
  public void writeMatrixMeta(AMMatrixMetaManager matrixMetaManager) throws IOException {
    try{
      matrixMetaLock.lock();
      String matrixMetaFile = getMatrixMetaFile();
      String tmpFile = getTmpMatrixMetaFile(matrixMetaFile);
      Path tmpPath = new Path(writeDir, tmpFile);
      FSDataOutputStream outputStream = fs.create(tmpPath);
      matrixMetaManager.serialize(outputStream);
      outputStream.close();


      Path matrixFilePath = new Path(writeDir, matrixMetaFile);
      HdfsUtil.rename(tmpPath, matrixFilePath, fs);
      if (lastMatrixMetaFilePath != null) {
        fs.delete(lastMatrixMetaFilePath, false);
      }
      lastMatrixMetaFilePath = matrixFilePath;
      
    } finally {
      matrixMetaLock.unlock();
    }
  }
  
  private String getMatrixMetaFile() {
    return matrixMetaFileNamePrefix + fileNameSeparator + System.nanoTime();
  }
  
  private String getTmpMatrixMetaFile(String matrixMetaFileName){
    return tmpPrefix + matrixMetaFileName;
  }
  
  /**
   * load matrix meta from file
   * @return MatrixMetaManager matrix meta storage
   * @throws IOException, ClassNotFoundException, InvalidParameterException 
   */
  public AMMatrixMetaManager loadMatrixMeta() throws IOException, ClassNotFoundException, InvalidParameterException {
    try{
      matrixMetaLock.lock();
      Path matrixFilePath = null;
      try{
        matrixFilePath = findFilePathUsePrefix(matrixMetaFileNamePrefix);
      } catch (Exception x) {
        LOG.error("find matrix meta file failed.", x);
        return null;
      }
      
      if(matrixFilePath == null) {
        return null;
      }
      
      FSDataInputStream inputStream = fs.open(new Path(writeDir, matrixFilePath));
      AMMatrixMetaManager matrixMetaManager = new AMMatrixMetaManager(context);
      matrixMetaManager.deserialize(inputStream);
      return matrixMetaManager;
    } finally {
      matrixMetaLock.unlock();
    }
  }
  
  private Path findFilePathUsePrefix(final String prefix) throws FileNotFoundException, IOException {
    FileStatus[] files = fs.listStatus(writeDirPath, new PathFilter() {
      @Override
      public boolean accept(Path path) {
        return path.getName().startsWith(prefix);
      }
    });
    
    return chooseMaxTsFilePath(files);
  }
  
  private Path chooseMaxTsFilePath(FileStatus[] files){
    if(files == null || files.length == 0) {
      return null;
    }
    
    int size = files.length;
    long maxTs = 0;
    int choosedFileIndex = 0;
    for(int i = 0; i < size; i++) {
      String [] nameSplits = files[i].getPath().getName().split(fileNameSeparator);
      if(nameSplits.length == 2) {
        long fileTs = Long.parseLong(nameSplits[1]);
        if(maxTs < fileTs) {
          choosedFileIndex = i;
          maxTs = fileTs;
        }
      }
    }
    
    return files[choosedFileIndex].getPath();
  }
  
  class Writter extends Thread{
    @Override
    public void run(){
      while(!stopped.get() && !Thread.interrupted()) {
        long startTs = System.currentTimeMillis();
        //write task meta
        try {
          writeTaskMeta(context.getTaskManager());
        } catch (IOException e) {
          if(Thread.interrupted()) {
            LOG.warn("write task meta is interupted");
          } else {
            LOG.error("write task meta file failed.", e);
          }
        } 
        
        //write ps meta
        try {
          writePSMeta(context.getParameterServerManager());
        } catch (IOException e) {
          if(Thread.interrupted()) {
            LOG.warn("write ps meta is interupted");
          } else {
            LOG.error("write ps meta file failed.", e);
          }
        } 
        
        long useTime = System.currentTimeMillis() - startTs;
        if(useTime < writeIntervalMS) {
          try {
            Thread.sleep(writeIntervalMS - useTime);
          } catch (InterruptedException e) {

          }
        }
      }
    }
  }
  
  private String getTaskMetaTmpeFile(String taskMetaFileName) {
    return tmpPrefix + taskMetaFileName;
  }
  
  private String getPSMetaTmpeFile(String psMetaFileName) {
    return tmpPrefix + psMetaFileName;
  }
  
  private String getTaskMetaFile() {
    return taskMetaFileNamePrefix + fileNameSeparator + System.nanoTime();
  }
  
  private String getPsMetaFile() {
    return psMetaFileNamePrefix + fileNameSeparator + System.nanoTime();
  }
  
  /**
   * write task meta to file
   * @param taskManager task meta storage
   * @throws IOException
   */
  public void writeTaskMeta(AMTaskManager taskManager) throws IOException {
    try{
      taskMetaLock.lock();
      //generate a temporary file
      String taskMetaFile = getTaskMetaFile();
      String tmpFile = getTaskMetaTmpeFile(taskMetaFile);
      Path tmpPath = new Path(writeDir, tmpFile);
      
      //write task meta to the temporary file first
      FSDataOutputStream outputStream = fs.create(tmpPath);
      taskManager.serialize(outputStream);
      outputStream.close();
      
      //rename the temporary file to the final file
      Path taskMetaFilePath = new Path(writeDir, taskMetaFile);
      HdfsUtil.rename(tmpPath, taskMetaFilePath, fs);
      //if last final task file exist, remove it
      if (lasttaskMetaFilePath != null) {
        fs.delete(lasttaskMetaFilePath, false);
      }

      lasttaskMetaFilePath = taskMetaFilePath;
    } finally {
      taskMetaLock.unlock();
    }
  }
  
  /**
   * load task meta from file
   * @return AMTaskManager task meta storage
   * @throws IOException
   */
  public AMTaskManager loadTaskMeta() throws IOException {
    try{
      taskMetaLock.lock();
      //find task meta file which has max timestamp
      Path taskMetaFilePath = null;
      try{
        taskMetaFilePath = findFilePathUsePrefix(taskMetaFileNamePrefix);
      } catch (Exception x) {
        LOG.error("find task meta file failed." , x);
        return null;
      }
      
      //if the file does not exist, just return null
      if(taskMetaFilePath == null) {
        return null;
      }
      
      //read task meta from file and deserialize it
      FSDataInputStream inputStream = fs.open(taskMetaFilePath);
      AMTaskManager taskManager = new AMTaskManager();
      taskManager.deserialize(inputStream);
      inputStream.close();
      return taskManager;
    } finally {
      taskMetaLock.unlock();
    }
  }

  /**
   * write ps meta to file
   * @param psManager ps meta storage
   * @throws IOException 
   */
  public void writePSMeta(ParameterServerManager psManager) throws IOException {
    try{
      psMetaLock.lock();
      //generate a temporary file
      String psMetaFile = getPsMetaFile();
      String tmpFile = getPSMetaTmpeFile(psMetaFile);
      Path tmpPath = new Path(writeDir, tmpFile);
      FSDataOutputStream outputStream = fs.create(tmpPath);
      
      //write ps meta to the temporary file first.
      Map<ParameterServerId, AMParameterServer> psMap = psManager.getParameterServerMap();
      outputStream.writeInt(psMap.size());
      PSAttemptId attemptId = null;
      int nextAttemptIndex = 0;
      for(Entry<ParameterServerId, AMParameterServer> entry:psMap.entrySet()) {
        outputStream.writeInt(entry.getKey().getIndex());
        attemptId = entry.getValue().getRunningAttemptId();
        nextAttemptIndex = entry.getValue().getNextAttemptNumber();
        if(attemptId != null) {
          nextAttemptIndex = attemptId.getIndex();
        }
        outputStream.writeInt(nextAttemptIndex);
      }
      
      outputStream.close();
      
      //rename the temporary file to the final file
      Path psMetaFilePath = new Path(writeDir, psMetaFile);
      HdfsUtil.rename(tmpPath, psMetaFilePath, fs);
      //if the old final file exist, just remove it
      if (lastPsMetaFilePath != null) {
        fs.delete(lastPsMetaFilePath, false);
      }

      lastPsMetaFilePath = psMetaFilePath;
    } finally {
      psMetaLock.unlock();
    }
  }
  
  /**
   * load ps meta from file
   * @return Map<ParameterServerId,Integer> psId to attempt index map
   * @throws IOException
   */
  public Map<ParameterServerId, Integer> loadPSMeta() throws IOException {
    try {
      psMetaLock.lock();
      //find ps meta file
      Path psMetaFilePath = null;
      try{
        psMetaFilePath = findFilePathUsePrefix(psMetaFileNamePrefix);
      } catch (Exception x) {
        LOG.error("find ps meta file failed." , x);
        return null;
      }
      
      //if ps meta file does not exist, just return null
      if(psMetaFilePath == null) {
        return null;
      }
      
      //read ps meta from file and deserialize it
      FSDataInputStream inputStream = fs.open(psMetaFilePath);
      int size = inputStream.readInt();
      Map<ParameterServerId, Integer> idToNextAttemptIndexMap = new HashMap<ParameterServerId, Integer>(size);
      for(int i = 0; i < size; i++) {
        idToNextAttemptIndexMap.put(new ParameterServerId(inputStream.readInt()), inputStream.readInt());
      }
      inputStream.close();
      return idToNextAttemptIndexMap;
    } finally {
      psMetaLock.unlock();
    }
  }
}
