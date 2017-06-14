package com.tencent.angel.psagent;

import com.google.protobuf.ServiceException;
import com.tencent.angel.client.AngelClient;
import com.tencent.angel.client.AngelClientFactory;
import com.tencent.angel.conf.AngelConfiguration;
import com.tencent.angel.conf.MatrixConfiguration;
import com.tencent.angel.exception.AngelException;
import com.tencent.angel.exception.InvalidParameterException;
import com.tencent.angel.localcluster.LocalClusterContext;
import com.tencent.angel.ml.math.TVector;
import com.tencent.angel.ml.math.vector.DenseIntVector;
import com.tencent.angel.ml.matrix.MatrixContext;
import com.tencent.angel.ml.matrix.psf.get.multi.GetRowsFunc;
import com.tencent.angel.ml.matrix.psf.get.multi.GetRowsParam;
import com.tencent.angel.ml.matrix.psf.get.multi.GetRowsResult;
import com.tencent.angel.protobuf.generated.MLProtos;
import com.tencent.angel.ps.PSAttemptId;
import com.tencent.angel.ps.ParameterServerId;
import com.tencent.angel.psagent.matrix.MatrixClient;
import com.tencent.angel.worker.Worker;
import com.tencent.angel.worker.WorkerAttemptId;
import com.tencent.angel.worker.WorkerGroupId;
import com.tencent.angel.worker.WorkerId;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.mapreduce.lib.input.CombineTextInputFormat;
import org.apache.log4j.PropertyConfigurator;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ExecutionException;

public class MatrixOpLogTest {
  private static final Log LOG = LogFactory.getLog(MatrixOpLogTest.class);
  private static final String LOCAL_FS = LocalFileSystem.DEFAULT_FS;
  private static final String TMP_PATH = System.getProperty("java.io.tmpdir", "/tmp");
  private AngelClient angelClient;
  private ParameterServerId psId;
  private PSAttemptId psAttempt0Id;
  private WorkerId workerId;
  private WorkerAttemptId workerAttempt0Id;

  static {
    PropertyConfigurator.configure("../conf/log4j.properties");
  }

  @Before
  public void setup() throws Exception {
    // set basic configuration keys
    Configuration conf = new Configuration();
    conf.setBoolean("mapred.mapper.new-api", true);
    conf.setBoolean(AngelConfiguration.ANGEL_JOB_OUTPUT_PATH_DELETEONEXIST, true);
    conf.set(AngelConfiguration.ANGEL_TASK_USER_TASKCLASS, DummyTask.class.getName());

    // use local deploy mode and dummy dataspliter
    conf.set(AngelConfiguration.ANGEL_DEPLOY_MODE, "LOCAL");
    conf.setBoolean(AngelConfiguration.ANGEL_AM_USE_DUMMY_DATASPLITER, true);
    conf.set(AngelConfiguration.ANGEL_INPUTFORMAT_CLASS, CombineTextInputFormat.class.getName());
    conf.set(AngelConfiguration.ANGEL_SAVE_MODEL_PATH, LOCAL_FS + TMP_PATH + "/out");
    conf.set(AngelConfiguration.ANGEL_TRAIN_DATA_PATH, LOCAL_FS + TMP_PATH + "/in");
    conf.set(AngelConfiguration.ANGEL_LOG_PATH, LOCAL_FS + TMP_PATH + "/log");

    conf.setInt(AngelConfiguration.ANGEL_WORKERGROUP_NUMBER, 1);
    conf.setInt(AngelConfiguration.ANGEL_PS_NUMBER, 1);
    conf.setInt(AngelConfiguration.ANGEL_WORKER_TASK_NUMBER, 2);

    // get a angel client
    angelClient = AngelClientFactory.get(conf);

    // add matrix
    MatrixContext mMatrix = new MatrixContext();
    mMatrix.setName("w1");
    mMatrix.setRowNum(100);
    mMatrix.setColNum(100000);
    mMatrix.setMaxRowNumInBlock(10);
    mMatrix.setMaxColNumInBlock(50000);
    mMatrix.setRowType(MLProtos.RowType.T_INT_DENSE);
    mMatrix.set(MatrixConfiguration.MATRIX_OPLOG_ENABLEFILTER, "false");
    mMatrix.set(MatrixConfiguration.MATRIX_HOGWILD, "true");
    mMatrix.set(MatrixConfiguration.MATRIX_AVERAGE, "false");
    mMatrix.set(MatrixConfiguration.MATRIX_OPLOG_TYPE, "DENSE_INT");
    angelClient.addMatrix(mMatrix);

    angelClient.startPSServer();
    angelClient.run();
    Thread.sleep(5000);
    psId = new ParameterServerId(0);
    psAttempt0Id = new PSAttemptId(psId, 0);
    WorkerGroupId workerGroupId = new WorkerGroupId(0);
    workerId = new WorkerId(workerGroupId, 0);
    workerAttempt0Id = new WorkerAttemptId(workerId, 0);
  }

  @Test
  public void testUDF() throws ServiceException, IOException, InvalidParameterException,
    AngelException, InterruptedException, ExecutionException {
    Worker worker = LocalClusterContext.get().getWorker(workerAttempt0Id).getWorker();
    MatrixClient w1Task0Client = worker.getPSAgent().getMatrixClient("w1", 0);
    MatrixClient w1Task1Client = worker.getPSAgent().getMatrixClient("w1", 1);
    int matrixW1Id = w1Task0Client.getMatrixId();
    List<Integer> rowIndexes = new ArrayList<Integer>();
    for(int i = 0; i < 100; i++) {
      rowIndexes.add(i);
    }

    GetRowsFunc func = new GetRowsFunc(new GetRowsParam(matrixW1Id, rowIndexes));

    int [] delta = new int[100000];
    for(int i = 0; i < 100000; i++) {
      delta[i] = 1;
    }

    //DenseIntVector deltaVec = new DenseIntVector(100000, delta);
    //deltaVec.setMatrixId(matrixW1Id);
    //deltaVec.setRowId(0);

    int index = 0;
    while(index++ < 10) {
      Map<Integer, TVector> rows = ((GetRowsResult) w1Task0Client.get(func)).getRows();
      for(Entry<Integer, TVector> rowEntry:rows.entrySet()) {
        LOG.info("index " + rowEntry.getKey() + " sum of w1 = " + sum((DenseIntVector) rowEntry.getValue()));
      }

      for(int i = 0; i < 100; i++) {
        DenseIntVector deltaVec = new DenseIntVector(100000, delta);
        deltaVec.setMatrixId(matrixW1Id);
        deltaVec.setRowId(i);

        w1Task0Client.increment(deltaVec);

        deltaVec = new DenseIntVector(100000, delta);
        deltaVec.setMatrixId(matrixW1Id);
        deltaVec.setRowId(i);
        w1Task1Client.increment(deltaVec);
      }

      w1Task0Client.clock().get();
      w1Task1Client.clock().get();
    }
  }

  private int sum(DenseIntVector vec){
    int [] values = vec.getValues();
    int sum = 0;
    for(int i = 0; i < values.length; i++) {
      sum += values[i];
    }
    return sum;
  }

  @After
  public void stop() throws AngelException {
    LOG.info("stop local cluster");
    angelClient.stop();
  }
}
