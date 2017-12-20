package com.tencent.angel.example.getValueOfIndex;


import com.tencent.angel.AppSubmitter;
import com.tencent.angel.client.AngelClient;
import com.tencent.angel.client.AngelClientFactory;
import com.tencent.angel.conf.MatrixConf;
import com.tencent.angel.ml.conf.MLConf;
import com.tencent.angel.ml.matrix.MatrixContext;
import com.tencent.angel.protobuf.generated.MLProtos;
import org.apache.hadoop.conf.Configuration;

public class GetValueOfIndexSubmmiter implements AppSubmitter {
  public static String DENSE_DOUBLE_MAT = "DENSE_DOUBLE_MAT";
  public static String SPARSE_DOUBLE_MAT = "SPARSE_DOUBLE_MAT";
  public static String LONG_SPARSE_DOUBLE_MAT = "LONG_SPARSE_DOUBLE_MAT";


  @Override
  public void submit(Configuration conf) throws Exception {
    AngelClient client = AngelClientFactory.get(conf);
    int feaNum = conf.getInt(MLConf.ML_FEATURE_NUM(), MLConf.DEFAULT_ML_FEATURE_NUM());

    MatrixContext dMat = new MatrixContext(DENSE_DOUBLE_MAT, 1, feaNum, -1,-1);
    dMat.setRowType(MLProtos.RowType.T_DOUBLE_DENSE);
    dMat.set(MatrixConf.MATRIX_AVERAGE, "true");

    MatrixContext sMat = new MatrixContext(SPARSE_DOUBLE_MAT, 1, feaNum, -1,-1);
    sMat.setRowType(MLProtos.RowType.T_DOUBLE_SPARSE);
    sMat.set(MatrixConf.MATRIX_AVERAGE, "true");

    MatrixContext lMat = new MatrixContext(LONG_SPARSE_DOUBLE_MAT, 1, feaNum, -1,-1);
    lMat.setRowType(MLProtos.RowType.T_DOUBLE_SPARSE_LONGKEY);
    lMat.set(MatrixConf.MATRIX_AVERAGE, "true");


    client.addMatrix(dMat);
    client.addMatrix(sMat);
    client.addMatrix(lMat);

    client.startPSServer();
    client.run();
    client.waitForCompletion();
    client.stop(0);
  }
}
