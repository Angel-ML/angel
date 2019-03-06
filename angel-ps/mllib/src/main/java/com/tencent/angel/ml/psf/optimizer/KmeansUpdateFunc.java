package com.tencent.angel.ml.psf.optimizer;

import com.tencent.angel.ml.math2.vector.Vector;
import com.tencent.angel.ps.storage.matrix.ServerPartition;
import com.tencent.angel.ps.storage.vector.ServerRow;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class KmeansUpdateFunc extends OptMMUpdateFunc{

  private static final Log LOG = LogFactory.getLog(KmeansUpdateFunc.class);

  public KmeansUpdateFunc() {
    super();
  }

  public KmeansUpdateFunc(int matId, int factor) {
    this(matId, factor, 1);
  }

  public KmeansUpdateFunc(int matId, int factor, int batchSize) {
    super(matId, new int[]{factor}, new double[]{batchSize});
  }
  @Override
  void update(ServerPartition partition, int factor, double[] scalars) {
    double batchSize = scalars[0];

    for (int f = 0; f < factor; f++) {
      ServerRow gradientServerRow = partition.getRow(f + factor);
      try {
        gradientServerRow.startWrite();
        Vector weight = partition.getRow(f).getSplit();
        Vector gradient = gradientServerRow.getSplit();

        if (batchSize > 1) {
          gradient.idiv(batchSize);
        }

        weight.iadd(gradient);

        gradient.clear();
      } finally {
        gradientServerRow.endWrite();
      }
    }
  }
}
