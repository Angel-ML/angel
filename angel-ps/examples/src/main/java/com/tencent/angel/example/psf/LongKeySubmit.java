package com.tencent.angel.example.psf;

import com.tencent.angel.AppSubmitter;
import com.tencent.angel.client.AngelClient;
import com.tencent.angel.client.AngelClientFactory;
import com.tencent.angel.conf.AngelConf;
import com.tencent.angel.ml.matrix.MatrixContext;
import com.tencent.angel.protobuf.generated.MLProtos;
import org.apache.hadoop.conf.Configuration;

/**
 * Created by payniexiao on 2017/8/21.
 */
public class LongKeySubmit implements AppSubmitter {
  @Override public void submit(Configuration conf) throws Exception {
    conf.setBoolean(AngelConf.ANGEL_AM_USE_DUMMY_DATASPLITER, true);
    AngelClient angelClient = AngelClientFactory.get(conf);
    int blockCol = conf.getInt("blockcol", 5000000);
    MatrixContext context = new MatrixContext("longkey_test", 1, 2100000000, 1, blockCol);
    context.setRowType(MLProtos.RowType.T_DOUBLE_SPARSE_LONGKEY);
    angelClient.addMatrix(context);
    angelClient.startPSServer();
    angelClient.run();
    angelClient.waitForCompletion();
    angelClient.stop(0);
  }
}
