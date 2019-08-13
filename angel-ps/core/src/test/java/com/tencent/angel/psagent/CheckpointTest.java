package com.tencent.angel.psagent;

import com.tencent.angel.client.AngelClient;
import com.tencent.angel.client.AngelClientFactory;
import com.tencent.angel.common.location.Location;
import com.tencent.angel.conf.AngelConf;
import com.tencent.angel.exception.AngelException;
import com.tencent.angel.ipc.TConnection;
import com.tencent.angel.ipc.TConnectionManager;
import com.tencent.angel.localcluster.LocalClusterContext;
import com.tencent.angel.master.MasterProtocol;
import com.tencent.angel.ml.math2.storage.LongDoubleSparseVectorStorage;
import com.tencent.angel.ml.math2.vector.LongDoubleVector;
import com.tencent.angel.ml.matrix.MatrixContext;
import com.tencent.angel.ml.matrix.RowType;
import com.tencent.angel.protobuf.ProtobufUtil;
import com.tencent.angel.protobuf.generated.PSMasterServiceProtos.PSErrorRequest;
import com.tencent.angel.ps.PSAttemptId;
import com.tencent.angel.ps.ParameterServer;
import com.tencent.angel.ps.ParameterServerId;
import com.tencent.angel.psagent.matrix.MatrixClient;
import com.tencent.angel.worker.Worker;
import com.tencent.angel.worker.WorkerAttemptId;
import com.tencent.angel.worker.WorkerGroupId;
import com.tencent.angel.worker.WorkerId;
import it.unimi.dsi.fastutil.ints.Int2ObjectMap;
import it.unimi.dsi.fastutil.ints.Int2ObjectOpenHashMap;
import it.unimi.dsi.fastutil.objects.ObjectIterator;
import java.util.Arrays;
import java.util.Random;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.mapreduce.lib.input.CombineTextInputFormat;
import org.apache.log4j.PropertyConfigurator;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class CheckpointTest {

  public static String DENSE_DOUBLE_MAT = "dense_double_mat";
  public static String DENSE_DOUBLE_MAT_COMP = "dense_double_mat_comp";
  public static String SPARSE_DOUBLE_MAT = "sparse_double_mat";
  public static String SPARSE_DOUBLE_MAT_COMP = "sparse_double_mat_comp";

  public static String DENSE_FLOAT_MAT = "dense_float_mat";
  public static String DENSE_FLOAT_MAT_COMP = "dense_float_mat_comp";
  public static String SPARSE_FLOAT_MAT = "sparse_float_mat";
  public static String SPARSE_FLOAT_MAT_COMP = "sparse_float_mat_comp";

  public static String DENSE_INT_MAT = "dense_int_mat";
  public static String DENSE_INT_MAT_COMP = "dense_int_mat_comp";
  public static String SPARSE_INT_MAT = "sparse_int_mat";
  public static String SPARSE_INT_MAT_COMP = "sparse_int_mat_comp";

  public static String DENSE_LONG_MAT = "dense_long_mat";
  public static String DENSE_LONG_MAT_COMP = "dense_long_mat_comp";
  public static String SPARSE_LONG_MAT = "sparse_long_mat";
  public static String SPARSE_LONG_MAT_COMP = "sparse_long_mat_comp";

  public static String DENSE_DOUBLE_LONG_MAT_COMP = "dense_double_long_mat_comp";
  public static String SPARSE_DOUBLE_LONG_MAT = "sparse_double_long_mat";
  public static String SPARSE_DOUBLE_LONG_MAT_COMP = "sparse_double_long_mat_comp";

  public static String DENSE_FLOAT_LONG_MAT_COMP = "dense_float_long_mat_comp";
  public static String SPARSE_FLOAT_LONG_MAT = "sparse_float_long_mat";
  public static String SPARSE_FLOAT_LONG_MAT_COMP = "sparse_float_long_mat_comp";

  public static String DENSE_INT_LONG_MAT_COMP = "dense_int_long_mat_comp";
  public static String SPARSE_INT_LONG_MAT = "sparse_int_long_mat";
  public static String SPARSE_INT_LONG_MAT_COMP = "sparse_int_long_mat_comp";

  public static String DENSE_LONG_LONG_MAT_COMP = "dense_long_long_mat_comp";
  public static String SPARSE_LONG_LONG_MAT = "sparse_long_long_mat";
  public static String SPARSE_LONG_LONG_MAT_COMP = "sparse_long_long_mat_comp";

  private static final Log LOG = LogFactory.getLog(CheckpointTest.class);
  private static final String LOCAL_FS = LocalFileSystem.DEFAULT_FS;
  private static final String TMP_PATH = System.getProperty("java.io.tmpdir", "/tmp");
  private AngelClient angelClient;
  private ParameterServerId psId;
  private PSAttemptId psAttempt0Id;
  private WorkerId workerId;
  private WorkerAttemptId workerAttempt0Id;

  int feaNum = 10000;
  int nnz = 1000;
  int rowNum = 5;
  int blockRowNum = 1;
  int blockColNum = 1000;
  double zero = 0.00000001;
  int modelSize = nnz;

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
    conf.set(AngelConf.ANGEL_SAVE_MODEL_PATH, "file:///F:\\test\\model_1");
    conf.set(AngelConf.ANGEL_TRAIN_DATA_PATH, LOCAL_FS + TMP_PATH + "/in");
    conf.set(AngelConf.ANGEL_LOG_PATH, LOCAL_FS + TMP_PATH + "/log");

    conf.setInt(AngelConf.ANGEL_WORKERGROUP_NUMBER, 1);
    conf.setInt(AngelConf.ANGEL_PS_NUMBER, 1);
    conf.setInt(AngelConf.ANGEL_WORKER_TASK_NUMBER, 1);
    //conf.setInt(AngelConf.ANGEL_MODEL_PARTITIONER_PARTITION_SIZE, 1000);

    conf.setInt(AngelConf.ANGEL_PSAGENT_CACHE_SYNC_TIMEINTERVAL_MS, 10);
    conf.setInt(AngelConf.ANGEL_WORKER_HEARTBEAT_INTERVAL_MS, 1000);
    conf.setInt(AngelConf.ANGEL_PS_HEARTBEAT_INTERVAL_MS, 1000);
    conf.setInt(AngelConf.ANGEL_WORKER_MAX_ATTEMPTS, 1);
    conf.setInt(AngelConf.ANGEL_PS_MAX_ATTEMPTS, 2);

    // get a angel client
    angelClient = AngelClientFactory.get(conf);

    // add sparse float matrix
    //MatrixContext siMat = new MatrixContext();
    //siMat.setName(SPARSE_INT_MAT);
    //siMat.setRowType(RowType.T_ANY_INTKEY_SPARSE);
    //siMat.setRowNum(1);
    //siMat.setValidIndexNum(100);
    //siMat.setColNum(10000000000L);
    //siMat.setValueType(Node.class);
    //siMat.setPartitionStorageClass(LongElementMapStorage.class);
    //siMat.setPartitionClass(CSRPartition.class);
    //angelClient.addMatrix(siMat);

    // add sparse long-key double matrix
    MatrixContext dLongKeysMatrix = new MatrixContext();
    dLongKeysMatrix.setName(SPARSE_DOUBLE_LONG_MAT);
    dLongKeysMatrix.setRowNum(1);
    dLongKeysMatrix.setColNum(feaNum);
    dLongKeysMatrix.setMaxColNumInBlock(feaNum / 3);
    dLongKeysMatrix.setRowType(RowType.T_DOUBLE_SPARSE_LONGKEY);
    dLongKeysMatrix.setValidIndexNum(modelSize);
    angelClient.addMatrix(dLongKeysMatrix);

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

  @Test
  public void testCheckpoint() throws Exception {

    Worker worker = LocalClusterContext.get().getWorker(workerAttempt0Id).getWorker();
    MatrixClient client = worker.getPSAgent().getMatrixClient(SPARSE_DOUBLE_LONG_MAT, 0);
    int matrixId = client.getMatrixId();

    ParameterServer ps = LocalClusterContext.get().getPS(psAttempt0Id).getPS();
    Location masterLoc =
        LocalClusterContext.get().getMaster().getAppMaster().getAppContext().getMasterService()
            .getLocation();
    TConnection connection = TConnectionManager.getConnection(ps.getConf());
    MasterProtocol master = connection.getMasterService(masterLoc.getIp(), masterLoc.getPort());

    LongDoubleVector deltaVec =
        new LongDoubleVector(feaNum, new LongDoubleSparseVectorStorage(feaNum, feaNum));
    for (int i = 0; i < feaNum; i++)
      deltaVec.set(i, i);
    deltaVec.setRowId(0);

    client.increment(deltaVec, true);
    //client1.clock().get();
    client.checkpoint(0).get();
    client.checkpoint(1).get();

    ps.stop(-1);
    PSErrorRequest request = PSErrorRequest.newBuilder()
        .setPsAttemptId(ProtobufUtil.convertToIdProto(psAttempt0Id))
        .setMsg("out of memory").build();
    master.psError(null, request);

    Thread.sleep(10000);

    LongDoubleVector row = (LongDoubleVector) client.getRow(0);
    for (int id = 0; id < feaNum; id++) {
      //System.out.println("id=" + id + ", value=" + row.get(id));
      Assert.assertTrue(row.get(id) == deltaVec.get(id));
    }
  }

  public int intersection(long[] A, long[] B) {
    if (A == null || B == null || A.length == 0 || B.length == 0) return 0;
    int count = 0;
    int pointerA = 0;
    int pointerB = 0;
    while (pointerA < A.length && pointerB < B.length) {
      if (A[pointerA] < B[pointerB]) pointerA++;
      else if (A[pointerA] > B[pointerB]) pointerB++;
      else {
        count++;
        pointerA++;
        pointerB++;
      }
    }

    return count;
  }

  @After
  public void stop() throws AngelException {
    LOG.info("stop local cluster");
    angelClient.stop();
  }
}
