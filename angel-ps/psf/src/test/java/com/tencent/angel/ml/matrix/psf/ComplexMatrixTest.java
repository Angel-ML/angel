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

package com.tencent.angel.ml.matrix.psf;

import com.tencent.angel.client.AngelClient;
import com.tencent.angel.client.AngelClientFactory;
import com.tencent.angel.conf.AngelConf;
import com.tencent.angel.localcluster.LocalClusterContext;
import com.tencent.angel.ml.matrix.MatrixContext;
import com.tencent.angel.ml.math2.utils.RowType;
//import com.tencent.angel.ml.matrix.psf.graph.adjacency.getneighbor.GetNeighbor;
//import com.tencent.angel.ml.matrix.psf.graph.adjacency.getneighbor.GetNeighborParam;
//import com.tencent.angel.ml.matrix.psf.graph.adjacency.getneighbor.GetNeighborResult;
//import com.tencent.angel.ml.matrix.psf.graph.adjacency.initneighbor.InitNeighbor;
//import com.tencent.angel.ml.matrix.psf.graph.adjacency.initneighbor.InitNeighborParam;
import com.tencent.angel.ps.PSAttemptId;
import com.tencent.angel.ps.ParameterServerId;
import com.tencent.angel.ps.storage.vector.element.IntArrayElement;
import com.tencent.angel.psagent.matrix.MatrixClient;
import com.tencent.angel.worker.Worker;
import com.tencent.angel.worker.WorkerAttemptId;
import com.tencent.angel.worker.WorkerGroupId;
import com.tencent.angel.worker.WorkerId;
import com.tencent.angel.worker.task.TaskId;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.mapreduce.lib.input.CombineTextInputFormat;
import org.apache.log4j.PropertyConfigurator;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/**
 * A simple ut for adj table
 */
public class ComplexMatrixTest {
  private static final Log LOG = LogFactory.getLog(ComplexMatrixTest.class);
  private static final String LOCAL_FS = LocalFileSystem.DEFAULT_FS;
  private static final String TMP_PATH = System.getProperty("java.io.tmpdir", "/tmp");
  private static AngelClient angelClient;
  private static WorkerGroupId group0Id;
  private static WorkerId worker0Id;
  private static WorkerAttemptId worker0Attempt0Id;
  private static TaskId task0Id;
  private static TaskId task1Id;
  private static ParameterServerId psId;
  private static PSAttemptId psAttempt0Id;

  public static final int nodeNum = 10000;
  public static final int maxNeighborNum = 1000;

  static {
    PropertyConfigurator.configure("../conf/log4j.properties");
  }


  @Before
  public void setup() throws Exception {
    try {
      // set basic configuration keys
      Configuration conf = new Configuration();
      conf.setBoolean("mapred.mapper.new-api", true);
      conf.setBoolean(AngelConf.ANGEL_JOB_OUTPUT_PATH_DELETEONEXIST, true);
      conf.set(AngelConf.ANGEL_TASK_USER_TASKCLASS, DummyTask.class.getName());

      // use local deploy mode and dummy dataspliter
      conf.set(AngelConf.ANGEL_DEPLOY_MODE, "LOCAL");
      conf.setBoolean(AngelConf.ANGEL_AM_USE_DUMMY_DATASPLITER, true);
      conf.set(AngelConf.ANGEL_INPUTFORMAT_CLASS, CombineTextInputFormat.class.getName());
      conf.set(AngelConf.ANGEL_SAVE_MODEL_PATH, LOCAL_FS + TMP_PATH + "/out");
      conf.set(AngelConf.ANGEL_TRAIN_DATA_PATH, LOCAL_FS + TMP_PATH + "/in");
      conf.set(AngelConf.ANGEL_LOG_PATH, LOCAL_FS + TMP_PATH + "/log");

      conf.setInt(AngelConf.ANGEL_WORKERGROUP_NUMBER, 1);
      conf.setInt(AngelConf.ANGEL_PS_NUMBER, 1);
      conf.setInt(AngelConf.ANGEL_WORKER_TASK_NUMBER, 2);

      conf.setInt(AngelConf.ANGEL_WORKER_HEARTBEAT_INTERVAL_MS, 1000);
      conf.setInt(AngelConf.ANGEL_PS_HEARTBEAT_INTERVAL_MS, 1000);

      // get a angel client
      angelClient = AngelClientFactory.get(conf);

      // add matrix
      MatrixContext mMatrix = new MatrixContext();
      mMatrix.setName("w1");
      mMatrix.setRowNum(1);
      mMatrix.setColNum(nodeNum);
      mMatrix.setMaxRowNumInBlock(1);
      mMatrix.setMaxColNumInBlock(nodeNum / 10);
      mMatrix.setRowType(RowType.T_ANY_INTKEY_DENSE);
      mMatrix.setValueType(IntArrayElement.class);
      angelClient.addMatrix(mMatrix);

      MatrixContext mMatrix2 = new MatrixContext();
      mMatrix2.setName("w2");
      mMatrix2.setRowNum(1);
      mMatrix2.setColNum(nodeNum);
      mMatrix2.setMaxRowNumInBlock(1);
      mMatrix2.setMaxColNumInBlock(nodeNum / 10);
      mMatrix2.setRowType(RowType.T_ANY_INTKEY_SPARSE);
      mMatrix2.setValueType(IntArrayElement.class);
      angelClient.addMatrix(mMatrix2);

      angelClient.startPSServer();
      angelClient.run();
      Thread.sleep(10000);
      group0Id = new WorkerGroupId(0);
      worker0Id = new WorkerId(group0Id, 0);
      worker0Attempt0Id = new WorkerAttemptId(worker0Id, 0);
      task0Id = new TaskId(0);
      task1Id = new TaskId(1);
      psId = new ParameterServerId(0);
      psAttempt0Id = new PSAttemptId(psId, 0);
    } catch (Exception x) {
      LOG.error("setup failed ", x);
      throw x;
    }
  }

//  @Test
//  public void testInitAndGet() throws ExecutionException, InterruptedException {
//    Worker worker = LocalClusterContext.get().getWorker(worker0Attempt0Id).getWorker();
//    MatrixClient client1 = worker.getPSAgent().getMatrixClient("w2", 0);
//    int matrixW1Id = client1.getMatrixId();
//    // Generate graph data
//    Map<Integer, int []> adjMap = generateAdjTable(nodeNum, maxNeighborNum);
//
//    // Init graph adj table
//    InitNeighbor func = new InitNeighbor(new InitNeighborParam(matrixW1Id, adjMap));
//    client1.update(func);
//
//    int [] nodeIds = new int[adjMap.size()];
//    int i = 0;
//    for(int nodeId : adjMap.keySet()) {
//      nodeIds[i++] = nodeId;
//    }
//
//    // Get graph adj table from PS
//    GetNeighbor getFunc = new GetNeighbor(new GetNeighborParam(matrixW1Id, nodeIds, maxNeighborNum));
//    Map<Integer, int[]> getResults = ((GetNeighborResult) (client1.get(getFunc)))
//        .getNodeIdToNeighborIndices();
//
//    // Check the result
//    for(Entry<Integer, int[]> entry : getResults.entrySet()) {
//      Assert.assertArrayEquals(entry.getValue(), adjMap.get(entry.getKey()));
//    }
//  }

  private static Map<Integer, int[]> generateAdjTable(int nodeNum, int maxNeighborNum) {
    Random r = new Random();

    Map<Integer, int[]> adjTable = new HashMap<>(nodeNum);
    for(int i = 0; i < nodeNum; i++) {
      adjTable.put(i, chooseNeighbors(nodeNum, i, Math.abs(r.nextInt()) % (maxNeighborNum + 1), r));
    }
    return adjTable;
  }

  private static int[] chooseNeighbors(int nodeNum, int nodeId, int neighborNum, Random r) {
    if(neighborNum == 0) {
      return new int[0];
    }

    Set<Integer> neighborSet = new HashSet<>(neighborNum);
    while(true) {
      int chooseId = Math.abs(r.nextInt()) % nodeNum;
      if(chooseId == nodeId) {
        continue;
      }
      neighborSet.add(chooseId);
      if(neighborSet.size() == neighborNum)
        break;
    }

    int [] ret = new int[neighborNum];
    int i = 0;
    for(int neighbor : neighborSet) {
      ret[i++] = neighbor;
    }
    return ret;
  }

  private static String toString(int [][] adjTable) {
    int len = adjTable.length;
    StringBuilder b = new StringBuilder();
    for(int i = 0; i < len; i++) {
      b.append(i);
      b.append(":");
      int [] neighbors = adjTable[i];
      for(int j = 0; j < neighbors.length; j++) {
        b.append(neighbors[j]);
        if(j < neighbors.length - 1) {
          b.append(",");
        }
      }
      b.append("\n");
    }

    return b.toString();
  }

  @AfterClass
  public static void stop() throws Exception {
    angelClient.stop();
  }
}
