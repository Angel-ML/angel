package com.tencent.angel.graph;

import com.tencent.angel.client.AngelClient;
import com.tencent.angel.client.AngelClientFactory;
import com.tencent.angel.conf.AngelConf;
import com.tencent.angel.exception.AngelException;
import com.tencent.angel.graph.client.getnodefeats2.GetNodeFeats;
import com.tencent.angel.graph.client.getnodefeats2.GetNodeFeatsParam;
import com.tencent.angel.graph.client.getnodefeats2.GetNodeFeatsResult;
import com.tencent.angel.graph.client.initNeighbor3.InitNeighbor;
import com.tencent.angel.graph.client.initNeighbor3.InitNeighborParam;
import com.tencent.angel.graph.client.initnodefeats2.InitNodeFeats;
import com.tencent.angel.graph.client.initnodefeats2.InitNodeFeatsParam;
import com.tencent.angel.graph.client.sampleneighbor3.SampleNeighbor;
import com.tencent.angel.graph.client.sampleneighbor3.SampleNeighborParam;
import com.tencent.angel.graph.client.sampleneighbor3.SampleNeighborResult;
import com.tencent.angel.graph.data.Node;
import com.tencent.angel.localcluster.LocalClusterContext;
import com.tencent.angel.ml.math2.VFactory;
import com.tencent.angel.ml.math2.vector.IntFloatVector;
import com.tencent.angel.ml.matrix.MatrixContext;
import com.tencent.angel.ml.math2.utils.RowType;
import com.tencent.angel.ps.PSAttemptId;
import com.tencent.angel.ps.ParameterServerId;
import com.tencent.angel.psagent.matrix.MatrixClient;
import com.tencent.angel.worker.Worker;
import com.tencent.angel.worker.WorkerAttemptId;
import com.tencent.angel.worker.WorkerGroupId;
import com.tencent.angel.worker.WorkerId;
import it.unimi.dsi.fastutil.ints.Int2FloatMap;
import it.unimi.dsi.fastutil.longs.Long2ObjectMap;
import it.unimi.dsi.fastutil.longs.Long2ObjectMap.Entry;
import it.unimi.dsi.fastutil.longs.Long2ObjectOpenHashMap;
import it.unimi.dsi.fastutil.objects.ObjectIterator;
import java.util.Arrays;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.mapreduce.lib.input.CombineTextInputFormat;
import org.apache.log4j.PropertyConfigurator;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class GetNodeFeatsTest2 {
  public static String NODE = "node";

  private static final Log LOG = LogFactory.getLog(InitNeighborTest.class);
  private static final String LOCAL_FS = LocalFileSystem.DEFAULT_FS;
  private static final String TMP_PATH = System.getProperty("java.io.tmpdir", "/tmp");
  private AngelClient angelClient;
  private ParameterServerId psId;
  private PSAttemptId psAttempt0Id;
  private WorkerId workerId;
  private WorkerAttemptId workerAttempt0Id;

  int feaNum = 100000;
  int start = 100;
  int end = 1000000;
  int nnz = 1000;

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
    conf.setInt(AngelConf.ANGEL_PS_MAX_ATTEMPTS, 1);

    // get a angel client
    angelClient = AngelClientFactory.get(conf);

    // add sparse float matrix
    MatrixContext siMat = new MatrixContext();
    siMat.setName(NODE);
    siMat.setRowType(RowType.T_ANY_LONGKEY_SPARSE);
    siMat.setRowNum(1);
    siMat.setColNum(10);
    siMat.setMaxColNumInBlock(5);
    siMat.setMaxRowNumInBlock(1);
    //siMat.setValidIndexNum(100);
    //siMat.setColNum(10000000000L);
    siMat.setValueType(Node.class);
    //siMat.setPartitionStorageClass(LongElementMapStorage.class);
    //siMat.setPartitionClass(CSRPartition.class);
    angelClient.addMatrix(siMat);

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
  public void testCSR() throws Exception {
    Worker worker = LocalClusterContext.get().getWorker(workerAttempt0Id).getWorker();
    MatrixClient client = worker.getPSAgent().getMatrixClient(NODE, 0);
    int matrixId = client.getMatrixId();

    // Init node neighbors and feats
    long[] nodeIds = new long[6];
    IntFloatVector[] feats = new IntFloatVector[6];
    long [][] neighbors = new long[6][];

    Long2ObjectOpenHashMap<long[]> idToNeighbors = new Long2ObjectOpenHashMap<>();

    nodeIds[0] = 1;
    neighbors[0] = new long[]{2,3,4,5,8};
    idToNeighbors.put(nodeIds[0], neighbors[0]);
    feats[0] = VFactory.denseFloatVector(5);
    feats[0].set(0, 0.2f);
    feats[0].set(1, 0.3f);
    feats[0].set(2, 0.4f);
    feats[0].set(3, 0.5f);
    feats[0].set(4, 0.6f);

    nodeIds[1] = 2;
    neighbors[1] = new long[]{1,3,4,5,8};
    idToNeighbors.put(nodeIds[1], neighbors[1]);
    feats[1] = VFactory.sparseFloatVector(5, 2);
    feats[1].set(1, 0.4f);
    feats[1].set(3, 0.5f);

    nodeIds[2] = 3;
    neighbors[2] = new long[]{1,2,4,5,8};
    idToNeighbors.put(nodeIds[2], neighbors[2]);
    feats[2] = VFactory.sortedFloatVector(5, 3);
    feats[2].set(0, 0.4f);
    feats[2].set(1, 0.5f);
    feats[2].set(4, 0.6f);

    nodeIds[3] = 4;
    neighbors[3] = new long[]{1,2,3,5,8};
    idToNeighbors.put(nodeIds[3], neighbors[3]);
    feats[3] = VFactory.sparseFloatVector(5, 2);
    feats[3].set(4, 0.6f);
    feats[3].set(1, 0.5f);

    nodeIds[4] = 5;
    neighbors[4] = new long[]{1,2,3,4,8};
    idToNeighbors.put(nodeIds[4], neighbors[4]);
    feats[4] = VFactory.sparseFloatVector(5, 1);
    feats[4].set(2, 0.6f);

    nodeIds[5] = 8;
    neighbors[2] = new long[]{1,2,3,4,5};
    idToNeighbors.put(nodeIds[5], neighbors[5]);
    feats[5] = VFactory.sparseFloatVector(5,2);
    feats[5].set(0, 0.3f);
    feats[5].set(1, 0.4f);

    InitNeighbor initFunc = new InitNeighbor(new InitNeighborParam(matrixId, idToNeighbors));
    client.asyncUpdate(initFunc).get();

    InitNodeFeats func = new InitNodeFeats(new InitNodeFeatsParam(matrixId, nodeIds, feats));
    client.asyncUpdate(func).get();

    // Sample the neighbors
    nodeIds = new long[]{1, 2, 3, 4, 5, 6, 7, 8};
    GetNodeFeatsParam param = new GetNodeFeatsParam(matrixId, nodeIds);
    Long2ObjectOpenHashMap<IntFloatVector> result = ((GetNodeFeatsResult) (client
        .get(new GetNodeFeats(param)))).getResult();
    ObjectIterator<Long2ObjectMap.Entry<IntFloatVector>> iter = result
        .long2ObjectEntrySet().fastIterator();

    LOG.info("==============================sample neighbors result============================");
    Long2ObjectMap.Entry<IntFloatVector> entry;
    while (iter.hasNext()) {
      entry = iter.next();
      IntFloatVector vector = entry.getValue();
      if(vector.isDense()) {
        LOG.info("node " + entry.getLongKey() + " has a dense features");
        float [] values = vector.getStorage().getValues();
        for(int i = 0; i < values.length; i++) {
          LOG.info("feat index " + i + " values = " + values[i]);
        }
      } else if(vector.isSparse()) {
        LOG.info("node " + entry.getLongKey() + " has a sparse features");
        ObjectIterator<Int2FloatMap.Entry> valueIter = vector
            .getStorage().entryIterator();

        while(valueIter.hasNext()) {
          Int2FloatMap.Entry keyValue = valueIter.next();
          LOG.info("feat index " + keyValue.getIntKey() + " values = " + keyValue.getFloatValue());
        }
      } else {
        LOG.info("node " + entry.getLongKey() + " has a sorted features");
        int [] keys = vector.getStorage().getIndices();
        float [] values = vector.getStorage().getValues();
        for(int i = 0; i < values.length; i++) {
          LOG.info("feat index " + keys[i] + " values = " + values[i]);
        }
      }
    }

    SampleNeighborParam sampleParam = new SampleNeighborParam(matrixId, nodeIds, -1);
    Long2ObjectOpenHashMap<long[]> sampleResult = ((SampleNeighborResult) (client
        .get(new SampleNeighbor(sampleParam)))).getNodeIdToNeighbors();

    ObjectIterator<Entry<long[]>> sampleIter = sampleResult
        .long2ObjectEntrySet().fastIterator();

    LOG.info("==============================sample neighbors result============================");
    while (sampleIter.hasNext()) {
      Entry<long[]> sampleEntry = sampleIter.next();
      LOG.info(
          "node id = " + sampleEntry.getLongKey() + ", neighbors = " + Arrays.toString(sampleEntry.getValue()));
    }
  }

  @After
  public void stop() throws AngelException {
    LOG.info("stop local cluster");
    angelClient.stop();
  }
}
