package com.tencent.angel.example.psf;

import com.tencent.angel.AppSubmitter;
import com.tencent.angel.client.AngelClient;
import com.tencent.angel.client.AngelClientFactory;
import com.tencent.angel.conf.AngelConf;
import com.tencent.angel.conf.MatrixConf;
import com.tencent.angel.ml.matrix.MatrixContext;
import com.tencent.angel.ml.matrix.RowType;
import org.apache.hadoop.conf.Configuration;

public class PytorchPSFSubmit implements AppSubmitter {

  @Override
  public void submit(Configuration conf) throws Exception {
    conf.setBoolean(AngelConf.ANGEL_AM_USE_DUMMY_DATASPLITER, true);

    AngelClient angelClient = AngelClientFactory.get(conf);
    long col = conf.getLong("col", 100000000);
    long blockCol = conf.getLong("blockcol", -1);
    long modelSize = conf.getLong("model.size", 100000000);

    MatrixContext context = new MatrixContext("psf_test", 1, col, modelSize, 1, blockCol);
    context.setRowType(RowType.T_FLOAT_DENSE);
    context.set(MatrixConf.MATRIX_SAVE_PATH, conf.get("angel.save.model.path"));
    angelClient.addMatrix(context);
    angelClient.startPSServer();
    angelClient.run();
    angelClient.waitForCompletion();
    angelClient.stop(0);
  }
}
