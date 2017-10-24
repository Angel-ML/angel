package com.tencent.angel.ml.lda.psf;

import com.tencent.angel.PartitionKey;
import com.tencent.angel.ml.matrix.psf.update.enhance.PartitionUpdateParam;
import com.tencent.angel.ml.matrix.psf.update.enhance.UpdateFunc;
import com.tencent.angel.ml.matrix.psf.update.enhance.UpdateParam;
import com.tencent.angel.ps.impl.PSContext;
import com.tencent.angel.ps.impl.matrix.ServerDenseIntRow;
import com.tencent.angel.ps.impl.matrix.ServerRow;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.nio.IntBuffer;

public class UpdatePartFunc extends UpdateFunc {
  private final static Log LOG = LogFactory.getLog(UpdatePartFunc.class);

  public UpdatePartFunc(UpdateParam param) {
    super(param);
  }

  public UpdatePartFunc() {
    super(null);
  }

  @Override
  public void partitionUpdate(PartitionUpdateParam partParam) {
    CSRPartUpdateParam param = (CSRPartUpdateParam) partParam;

    PartitionKey pkey = partParam.getPartKey();

    pkey = PSContext.get().getMatrixPartitionManager().
        getPartition(pkey.getMatrixId(),
            pkey.getPartitionId()).getPartitionKey();

    while (param.buf.isReadable()) {
      int row = param.buf.readInt();
      int len = param.buf.readShort();

      ServerRow serverRow = PSContext.get().getMatrixPartitionManager().getRow(pkey, row);
      serverRow.getLock().writeLock().lock();
      ServerDenseIntRow denseIntRow = (ServerDenseIntRow) serverRow;
      IntBuffer buffer = denseIntRow.getData();
      for (int i = 0; i < len; i ++) {
        short key = param.buf.readShort();
        int val   = param.buf.readInt();
        buffer.put(key, buffer.get(key) + val);
      }
      serverRow.getLock().writeLock().unlock();
    }

    param.buf.release();
  }
}
