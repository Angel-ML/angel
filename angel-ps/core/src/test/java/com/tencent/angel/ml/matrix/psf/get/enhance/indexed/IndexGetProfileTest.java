package com.tencent.angel.ml.matrix.psf.get.enhance.indexed;

import com.tencent.angel.client.AngelClient;
import com.tencent.angel.client.AngelClientFactory;
import com.tencent.angel.conf.AngelConf;
import com.tencent.angel.localcluster.LocalClusterContext;
import com.tencent.angel.ml.math.vector.CompSparseDoubleVector;
import com.tencent.angel.ml.math.vector.DenseDoubleVector;
import com.tencent.angel.ml.math.vector.SparseDoubleVector;
import com.tencent.angel.ml.matrix.MatrixContext;
import com.tencent.angel.ml.matrix.MatrixOpLogType;
import com.tencent.angel.ml.matrix.RowType;
import com.tencent.angel.ml.matrix.psf.get.single.GetRowResult;
import com.tencent.angel.ps.PSAttemptId;
import com.tencent.angel.ps.ParameterServerId;
import com.tencent.angel.psagent.DummyTask;
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
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.Random;

public class IndexGetProfileTest {
  public static String DENSE_DOUBLE_MAT = "dense_double_mat";
  public static String SPARSE_DOUBLE_MAT = "sparse_double_mat";
  public static String SPARSE_DOUBLE_MAT_COMP = "sparse_double_mat_comp";

  public static String SPARSE_DOUBLE_LONG_MAT = "sparse_double_long_mat";
  public static String SPARSE_DOUBLE_LONG_MAT_COMP = "sparse_double_long_mat_comp";

  public static String DENSE_FLOAT_MAT = "dense_float_mat";
  public static String SPARSE_FLOAT_MAT = "sparse_float_mat";
  public static String SPARSE_FLOAT_MAT_COMP = "sparse_float_mat_comp";

  public static String DENSE_INT_MAT = "dense_int_mat";
  public static String SPARSE_INT_MAT = "sparse_int_mat";
  public static String SPARSE_INT_MAT_COMP = "sparse_int_mat_comp";

  private static final Log LOG = LogFactory.getLog(IndexGetFuncTest.class);
  private static final String LOCAL_FS = LocalFileSystem.DEFAULT_FS;
  private static final String TMP_PATH = System.getProperty("java.io.tmpdir", "/tmp");
  private AngelClient angelClient;
  private ParameterServerId psId;
  private PSAttemptId psAttempt0Id;
  private WorkerId workerId;
  private WorkerAttemptId workerAttempt0Id;

  int feaNum = 10000000;
  int nnz = 5000000;


  static {
    PropertyConfigurator.configure("../conf/log4j.properties");
  }

  @Before
  public void setup() throws Exception {
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

    // get a angel client
    angelClient = AngelClientFactory.get(conf);

    /*
    // add dense double matrix
    MatrixContext dMat = new MatrixContext();
    dMat.setName(DENSE_DOUBLE_MAT);
    dMat.setRowNum(1);
    dMat.setColNum(feaNum);
    dMat.setMaxRowNumInBlock(1);
    dMat.setMaxColNumInBlock(feaNum);
    dMat.setRowType(RowType.T_DOUBLE_DENSE);
    angelClient.addMatrix(dMat);
    */

    /*
    // add sparse double matrix
    MatrixContext sMat = new MatrixContext();
    sMat.setName(SPARSE_DOUBLE_MAT);
    sMat.setRowNum(1);
    sMat.setColNum(feaNum);
    sMat.setMaxRowNumInBlock(1);
    sMat.setMaxColNumInBlock(feaNum);
    sMat.setRowType(RowType.T_DOUBLE_SPARSE);
    sMat.setMatrixOpLogType(MatrixOpLogType.DENSE_DOUBLE);
    angelClient.addMatrix(sMat);
    */

    // add component sparse double matrix
    MatrixContext sCompMat = new MatrixContext();
    sCompMat.setName(SPARSE_DOUBLE_MAT_COMP);
    sCompMat.setRowNum(1);
    sCompMat.setColNum(feaNum);
    sCompMat.setMaxRowNumInBlock(1);
    sCompMat.setMaxColNumInBlock(feaNum);
    sCompMat.setRowType(RowType.T_DOUBLE_SPARSE_COMPONENT);
    sCompMat.setMatrixOpLogType(MatrixOpLogType.DENSE_DOUBLE);
    angelClient.addMatrix(sCompMat);

    // Start PS
    angelClient.startPSServer();
    // Start to run application
    angelClient.run();

    Thread.sleep(5000);

    psId = new ParameterServerId(0);
    psAttempt0Id = new PSAttemptId(psId, 0);

    WorkerGroupId workerGroupId = new WorkerGroupId(0);
    workerId = new WorkerId(workerGroupId, 0);
    workerAttempt0Id = new WorkerAttemptId(workerId, 0);
  }

  public void testDenseDoubleUDF() throws Exception {
    Worker worker = LocalClusterContext.get().getWorker(workerAttempt0Id).getWorker();
    MatrixClient client1 = worker.getPSAgent().getMatrixClient(DENSE_DOUBLE_MAT, 0);
    int matrixW1Id = client1.getMatrixId();

    int[] index = genIndexs(feaNum, nnz);

    DenseDoubleVector deltaVec = new DenseDoubleVector(feaNum);
    for (int i = 0; i < feaNum; i++)
      deltaVec.set(i, i);
    deltaVec.setRowId(0);

    client1.increment(deltaVec);
    client1.clock().get();

    long startTs = System.currentTimeMillis();
    for(int i = 0; i < 500; i++) {
      IndexGetFunc func = new IndexGetFunc(new IndexGetParam(matrixW1Id, 0, index));
      SparseDoubleVector row = (SparseDoubleVector) ((GetRowResult) client1.get(func)).getRow();
    }
    LOG.info("================100 index get use time = " + (System.currentTimeMillis() - startTs) + " ms ");

    //for (int id: index) {
    //  Assert.assertTrue(row.get(id) == deltaVec.get(id));
    //}
    //Assert.assertTrue(index.length == row.size());

  }


  public void testSparseDoubleUDF() throws Exception {

    Worker worker = LocalClusterContext.get().getWorker(workerAttempt0Id).getWorker();
    MatrixClient client1 = worker.getPSAgent().getMatrixClient(SPARSE_DOUBLE_MAT, 0);
    int matrixW1Id = client1.getMatrixId();

    int[] index = genIndexs(feaNum, nnz);

    DenseDoubleVector deltaVec = new DenseDoubleVector(feaNum);
    for (int i = 0; i < feaNum; i++)
      deltaVec.set(i, i);
    deltaVec.setRowId(0);

    client1.increment(deltaVec);
    client1.clock().get();

    long startTs = System.currentTimeMillis();
    for(int i = 0; i < 500; i++) {
      IndexGetFunc func = new IndexGetFunc(new IndexGetParam(matrixW1Id, 0, index));
      SparseDoubleVector row = (SparseDoubleVector) ((GetRowResult) client1.get(func)).getRow();
    }
    LOG.info("================500 index get use time = " + (System.currentTimeMillis() - startTs) + " ms ");

    //for (int id: index) {
      //System.out.println("id=" + id + ", value=" + row.get(id));
      //Assert.assertTrue(row.get(id) == deltaVec.get(id));
    //}

    //Assert.assertTrue(index.length == row.size());
  }

  @Test
  public void testSparseDoubleCompUDF() throws Exception {

    Worker worker = LocalClusterContext.get().getWorker(workerAttempt0Id).getWorker();
    MatrixClient client1 = worker.getPSAgent().getMatrixClient(SPARSE_DOUBLE_MAT_COMP, 0);
    int matrixW1Id = client1.getMatrixId();

    int[] index = genIndexs(feaNum, nnz);

    DenseDoubleVector deltaVec = new DenseDoubleVector(feaNum);
    for (int i = 0; i < feaNum; i++)
      deltaVec.set(i, i);
    deltaVec.setRowId(0);

    client1.increment(deltaVec);
    client1.clock().get();

    long startTs = System.currentTimeMillis();
    for(int i = 0; i < 500; i++) {
      IndexGetFunc func = new IndexGetFunc(new IndexGetParam(matrixW1Id, 0, index));
      CompSparseDoubleVector row = (CompSparseDoubleVector) ((GetRowResult) client1.get(func)).getRow();
    }
    LOG.info("");

    //for (int id: index) {
    //  System.out.println("id=" + id + ", value=" + row.get(id));
     // Assert.assertTrue(row.get(id) == deltaVec.get(id));
    //}

    //Assert.assertTrue(index.length == row.size());
  }

  public static int[] genIndexs(int feaNum, int nnz) {
    int[] sortedIndex = new int[nnz];
    Random random = new Random(System.currentTimeMillis());
    sortedIndex[0] = random.nextInt(feaNum/nnz);
    for (int i = 1; i < nnz; i ++) {
      int rand = random.nextInt( (feaNum - sortedIndex[i-1]) / (nnz - i) );
      if (rand==0) rand = 1;
      sortedIndex[i] = rand + sortedIndex[i-1];
    }
    return sortedIndex;
  }
}
