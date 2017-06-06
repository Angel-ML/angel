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

package com.tencent.angel.example;

import com.tencent.angel.client.AngelClient;
import com.tencent.angel.ml.math.vector.DenseDoubleVector;
import com.tencent.angel.psagent.PSAgent;
import com.tencent.angel.psagent.matrix.MatrixClient;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.util.concurrent.CountDownLatch;


public class PSAgentClientTest {
  private static final Log LOG = LogFactory.getLog(AngelClient.class);
  private static final String matrixPrefix = "w_";

  public static void main(String[] args) {
    String masterIp = args[0];
    int masterPort = Integer.valueOf(args[1]);
    int matrixNum = Integer.valueOf(args[2]);
    int matrixDim = Integer.valueOf(args[3]);
    int taskNum = Integer.valueOf(args[4]);
    int iteration = Integer.valueOf(args[5]);
    LOG.debug("master ip=" + masterIp + ", master port=" + masterPort + ", matrix number="
        + matrixNum + ", matrix dimension=" + matrixDim + ", task number=" + taskNum
        + ", iteration=" + iteration);
    PSAgent psAgent = new PSAgent(masterIp, masterPort, 0);
    try {
      psAgent.initAndStart();
    } catch (Exception e) {
      LOG.debug("init psAgent faliled, ", e);
      return;
    }
    CountDownLatch counter = new CountDownLatch(taskNum);
    Task[] tasks = new Task[taskNum];
    for (int i = 0; i < taskNum; i++) {
      tasks[i] = new Task(matrixNum, matrixDim, iteration, psAgent, i);
      tasks[i].setName("usertask-" + i);
      tasks[i].start();
    }

    try {
      counter.await();
    } catch (InterruptedException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }
    LOG.info("all task run over");
    psAgent.stop();
    psAgent = null;
  }

  public static class Task extends Thread {
    private final int matrixNum;
    private final int matrixDim;
    private final int iteration;
    private final PSAgent psAgent;
    private final int index;

    public Task(int matrixNum, int matrixDim, int iteration, PSAgent psAgent, int index) {
      this.matrixNum = matrixNum;
      this.matrixDim = matrixDim;
      this.iteration = iteration;
      this.psAgent = psAgent;
      this.index = index;
    }

    @Override
    public void run() {
      double[] delta = new double[matrixDim];
      for (int i = 0; i < matrixDim; i++) {
        delta[i] = 1;
      }

      for (int i = 0; i < iteration; i++) {
        LOG.info("task-" + index + " start to run iteration " + i);
        for (int j = 0; j < matrixNum; j++) {
          try {
            MatrixClient client = psAgent.getMatrixClient(matrixPrefix + j, index);
            DenseDoubleVector row = (DenseDoubleVector) client.getRow(0);
            LOG.info("matrix name=" + matrixPrefix + j + ", rowIndex=0, length="
                + row.getValues().length + " sum=" + sum(row.getValues()));

            DenseDoubleVector deltaRow = new DenseDoubleVector(matrixDim, delta);
            deltaRow.setRowId(0);
            client.increment(deltaRow);
            client.clock();
            LOG.info("matrix name=" + matrixPrefix + j + " clocked!");
          } catch (Exception e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
          }
        }

      }
    }

    public double sum(double[] args) {
      double sum = 0.0;
      for (int i = 0; i < args.length; i++) {
        sum += args[i];
      }
      return sum;
    }
  }
}
