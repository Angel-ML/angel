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

package com.tencent.angel.master.data;

import com.tencent.angel.conf.AngelConf;
import com.tencent.angel.master.app.AMContext;
import com.tencent.angel.split.SplitClassification;
import com.tencent.angel.utils.HdfsUtil;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.util.ReflectionUtils;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

/**
 * Training date split tool, which splits the training data into blocks of roughly equal size
 * according to the number of workers and tasks
 */
public class DataSpliter {
  private static final Log LOG = LogFactory.getLog(DataSpliter.class);
  protected final AMContext context;

  /** index to SplitClassification map */
  private final Map<Integer, SplitClassification> splitClassifications;

  /** use new version MapReduce API */
  private final boolean useNewAPI;

  private final static int maxLocationLimit = 10;
  private int splitNum;

  public DataSpliter(AMContext context) {
    this(context, new HashMap<Integer, SplitClassification>());
  }

  public DataSpliter(AMContext context, Map<Integer, SplitClassification> splits) {
    this.context = context;
    this.splitClassifications = splits;
    useNewAPI = context.getConf().getBoolean("mapred.mapper.new-api", false);
  }

  /**
   * Split training data in the input directory into roughly equal pieces, and then group them by
   * workergroup
   * 
   * @return Map<Integer,SplitClassification> splits group map
   */
  public Map<Integer, SplitClassification> generateSplits() throws IOException,
      InterruptedException, ClassNotFoundException {
    Configuration conf = context.getConf();
    String trainDataPath = conf.get(AngelConf.ANGEL_TRAIN_DATA_PATH);
    if (trainDataPath != null) {
      conf.set("mapreduce.input.fileinputformat.inputdir", trainDataPath);
    }

    // Calculate how many splits we need. As each task handles a separate split of data, so we want
    // the number of splits equal to the number of tasks
    int workergroupNumber =
        conf.getInt(AngelConf.ANGEL_WORKERGROUP_NUMBER,
            AngelConf.DEFAULT_ANGEL_WORKERGROUP_NUMBER);
    int taskNumInWorker =
        conf.getInt(AngelConf.ANGEL_WORKER_TASK_NUMBER,
            AngelConf.DEFAULT_ANGEL_WORKER_TASK_NUMBER);
    int splitNum = workergroupNumber * taskNumInWorker;
    LOG.info("expected split number=" + splitNum);

    if (!useNewAPI) {
      LOG.info("use old mapreduce api");
      // split data
      org.apache.hadoop.mapred.InputSplit[] splitArray = generateSplitsUseOldAPI(conf, splitNum);
      LOG.info("splits number=" + splitArray.length);

      if (LOG.isDebugEnabled()) {
        int num = splitArray.length;
        for (int i = 0; i < num; i++) {
          LOG.debug("split [" + i + "]=" + splitArray[i].getLength());
        }
      }

      // dispatch the splits to workergroups
      dispatchSplitsUseLocation(splitArray, workergroupNumber, taskNumInWorker);
    } else {
      LOG.info("use new mapreduce api");
      // split data
      List<org.apache.hadoop.mapreduce.InputSplit> splitsNewAPI =
          generateSplitsUseNewAPI(conf, splitNum);
      LOG.info("splits number=" + splitsNewAPI.size());
      if (LOG.isDebugEnabled()) {
        int num = splitsNewAPI.size();
        for (int i = 0; i < num; i++) {
          LOG.debug("split [" + i + "]=" + splitsNewAPI.get(i).getLength());
        }
      }

      // dispatch the splits to workergroups
      dispatchSplitsUseLocation(splitsNewAPI, workergroupNumber, taskNumInWorker);
    }

    return splitClassifications;
  }

  private List<org.apache.hadoop.mapreduce.InputSplit> generateSplitsUseNewAPI(Configuration conf,
      int expectedSplitNum) throws IOException, ClassNotFoundException, InterruptedException {
    String jobIDStr = conf.get(AngelConf.ANGEL_JOB_ID);
    JobID jobID = JobID.forName(jobIDStr);
    JobContext jobConf = new JobContextImpl(new JobConf(conf), jobID);

    jobConf.getCredentials().addAll(UserGroupInformation.getCurrentUser().getCredentials());
    // Set split minsize and maxsize to expected split size. We need to get the total size of data
    // first, then divided by expected split number
    long totalInputFileSize = HdfsUtil.getInputFileTotalSize(jobConf);
    LOG.info("totalInputFileSize=" + totalInputFileSize);

    jobConf.getConfiguration().setLong(
        org.apache.hadoop.mapreduce.lib.input.FileInputFormat.SPLIT_MINSIZE,
        totalInputFileSize / expectedSplitNum);
    jobConf.getConfiguration().setLong(
        org.apache.hadoop.mapreduce.lib.input.FileInputFormat.SPLIT_MAXSIZE,
        totalInputFileSize / expectedSplitNum);

    // get input format class from configuration and then instantiation a input format object
    String inputFormatClassName =
        jobConf.getConfiguration().get(AngelConf.ANGEL_INPUTFORMAT_CLASS,
            AngelConf.DEFAULT_ANGEL_INPUTFORMAT_CLASS);
    org.apache.hadoop.mapreduce.InputFormat<?, ?> input =
        (org.apache.hadoop.mapreduce.InputFormat<?, ?>) ReflectionUtils.newInstance(
            Class.forName(inputFormatClassName), conf);
    LOG.debug("inputFormatClassName=" + inputFormatClassName);

    // split data
    return input.getSplits(jobConf);
  }

  private org.apache.hadoop.mapred.InputSplit[] generateSplitsUseOldAPI(Configuration conf,
      int expectedSplitNum) throws ClassNotFoundException, IOException {
    // Set split minsize and maxsize to expected split size. We need to get the total size of data
    // first, then divided by expected split number
    JobConf jobConf = new JobConf(conf);
    long totalInputFileSize = HdfsUtil.getInputFileTotalSize(jobConf);
    LOG.info("totalInputFileSize=" + totalInputFileSize);

    jobConf.setLong(org.apache.hadoop.mapreduce.lib.input.FileInputFormat.SPLIT_MINSIZE,
        totalInputFileSize / expectedSplitNum);
    jobConf.setLong(org.apache.hadoop.mapreduce.lib.input.FileInputFormat.SPLIT_MAXSIZE,
        totalInputFileSize / expectedSplitNum);

    // get input format class from configuration and then instantiation a input format object
    String inputFormatClassName =
        jobConf.get(AngelConf.ANGEL_INPUTFORMAT_CLASS,
            AngelConf.DEFAULT_ANGEL_INPUTFORMAT_CLASS);
    InputFormat<?, ?> input =
        (InputFormat<?, ?>) ReflectionUtils.newInstance(Class.forName(inputFormatClassName), conf);
    LOG.debug("inputFormatClassName=" + inputFormatClassName);

    // split data
    return input.getSplits(jobConf, splitNum);
  }

  private void dispatchSplitsUseLocation(List<org.apache.hadoop.mapreduce.InputSplit> splitsNewAPI,
      int groupNumber, int groupItemNumber) throws IOException, InterruptedException {
    splitNum = splitsNewAPI.size();

    // Since the actual split size is sometimes not exactly equal to the expected split size, we
    // need to fine tune the number of workergroup and task based on the actual split number
    int estimatedGroupNum = (splitNum + groupItemNumber - 1) / groupItemNumber;
    int base = 0;

    // Dispatch data splits to workergroups, each SplitClassification corresponds to a workergroup.
    // Record the location information for the splits in order to data localized schedule
    for (int i = 0; i < estimatedGroupNum; i++) {
      List<String> locationList = new ArrayList<String>(maxLocationLimit);
      List<org.apache.hadoop.mapreduce.InputSplit> splitList =
          new ArrayList<org.apache.hadoop.mapreduce.InputSplit>();

      base = i * groupItemNumber;
      for (int j = 0; j < groupItemNumber && (base < splitNum); j++, base++) {
        splitList.add(splitsNewAPI.get(base));
        String[] locations = splitsNewAPI.get(base).getLocations();
        for (int k = 0; k < locations.length && locationList.size() < maxLocationLimit; k++) {
          locationList.add(locations[k]);
        }
      }

      SplitClassification splitClassification =
          new SplitClassification(null, splitList, locationList.toArray(new String[locationList
              .size()]), true);
      splitClassifications.put(i, splitClassification);
    }
  }

  private void dispatchSplitsUseLocation(InputSplit[] splitArray, int groupNumber,
      int groupItemNumber) throws IOException {
    splitNum = splitArray.length;

    // Since the actual split size is sometimes not exactly equal to the expected split size, we
    // need to fine tune the number of workergroup and task based on the actual split number
    int estimatedGroupNum = (splitNum + groupItemNumber - 1) / groupItemNumber;
    int base = 0;

    // Dispatch data splits to workergroups, each SplitClassification corresponds to a workergroup.
    // Record the location information for the splits in order to data localized schedule
    for (int i = 0; i < estimatedGroupNum; i++) {
      List<String> locationList = new ArrayList<String>(maxLocationLimit);
      List<org.apache.hadoop.mapred.InputSplit> splitList =
          new ArrayList<org.apache.hadoop.mapred.InputSplit>();

      base = i * groupItemNumber;
      for (int j = 0; j < groupItemNumber && (base < splitNum); j++, base++) {
        splitList.add(splitArray[base]);
        String[] locations = splitArray[base].getLocations();
        for (int k = 0; k < locations.length && locationList.size() < maxLocationLimit; k++) {
          locationList.add(locations[k]);
        }
      }

      SplitClassification splitClassification =
          new SplitClassification(splitList, null, locationList.toArray(new String[locationList
              .size()]), useNewAPI);
      splitClassifications.put(i, splitClassification);
    }
  }

  public String[] getSplitLocations(int index) {
    return splitClassifications.get(index).getLocations();
  }

  public SplitClassification getSplits(int index) {
    return splitClassifications.get(index);
  }

  public int getSplitNum() {
    return splitNum;
  }

  /**
   * write data splits to a output stream
   * 
   * @param outputStream output stream
   * @throws IOException
   */
  public void serialize(FSDataOutputStream outputStream) throws IOException {
    outputStream.writeInt(splitNum);
    outputStream.writeInt(splitClassifications.size());
    for (Entry<Integer, SplitClassification> entry : splitClassifications.entrySet()) {
      outputStream.writeInt(entry.getKey());
      entry.getValue().serialize(outputStream);
    }
  }

  /**
   * read data splits from a input stream
   * 
   * @param inputStream input stream
   * @throws IOException
   */
  public void deserialize(FSDataInputStream inputStream) throws IOException, ClassNotFoundException {
    splitNum = inputStream.readInt();
    int size = inputStream.readInt();

    for (int i = 0; i < size; i++) {
      int index = inputStream.readInt();
      SplitClassification split = new SplitClassification();
      split.deserialize(inputStream);
      splitClassifications.put(index, split);
    }
    inputStream.close();
  }
}
