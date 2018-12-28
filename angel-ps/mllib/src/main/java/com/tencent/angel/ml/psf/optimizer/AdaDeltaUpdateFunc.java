package com.tencent.angel.ml.psf.optimizer;

import com.tencent.angel.ml.math2.ufuncs.OptFuncs;
import com.tencent.angel.ml.math2.vector.Vector;
import com.tencent.angel.ml.matrix.psf.update.base.PartitionUpdateParam;
import com.tencent.angel.ml.matrix.psf.update.enhance.MMUpdateParam;
import com.tencent.angel.ps.storage.matrix.ServerPartition;
import com.tencent.angel.ps.storage.vector.ServerRow;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class AdaDeltaUpdateFunc extends OptMMUpdateFunc {
    private static final Log LOG = LogFactory.getLog(AdaDeltaUpdateFunc.class);

    public AdaDeltaUpdateFunc() {
        super();
    }

    public AdaDeltaUpdateFunc(int matId, int factor, double epsilon, double beta, double lr, double regL1Param, double regL2Param, int epoch) {
        super(matId, new int[]{factor}, new double[]{epsilon, beta, lr, regL1Param, regL2Param, epoch, 1});
    }

    public AdaDeltaUpdateFunc(int matId, int factor, double epsilon, double beta, double lr, double regL1Param, double regL2Param, int epoch, int batchSize) {
        super(matId, new int[]{factor}, new double[]{epsilon, beta, lr, regL1Param, regL2Param, epoch, batchSize});
    }

    @Override
    public void update(ServerPartition partition, int factor, double[] scalars) {
        double epsilon = scalars[0];
        double beta = scalars[1];
        double lr = scalars[2];
        double l1RegParam = scalars[3];
        double l2RegParam = scalars[4];
        double epoch = (int) scalars[5];
        double batchSize = (int) scalars[6];

        for (int f = 0; f < factor; f++) {
            ServerRow gradientServerRow = partition.getRow(f + 3 * factor);
            try {
                gradientServerRow.startWrite();
                Vector weight = partition.getRow(f).getSplit();
                Vector square1 = partition.getRow(f + factor).getSplit();
                Vector square2 = partition.getRow(f + 2 * factor).getSplit();
                Vector gradient = gradientServerRow.getSplit();

                if (batchSize > 1) {
                    gradient.idiv(batchSize);
                }

                OptFuncs.iexpsmoothing2(square1, gradient, beta);
                Vector hessian = OptFuncs.adadeltahessian(square1, square2);

                if (l2RegParam != 0) {
                    gradient.iaxpy(weight, l2RegParam);
                }

                OptFuncs.iadadeltadelta(gradient, hessian, l2RegParam);
                weight.isub(gradient);

                if (l1RegParam != 0) {
                    OptFuncs.iadadeltathredshold(weight, hessian, l1RegParam, l2RegParam);
                }

                OptFuncs.iexpsmoothing2(square2, gradient, beta);

                gradient.clear();
            } finally {
                gradientServerRow.endWrite();
            }

        }
    }
}
