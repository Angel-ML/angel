package com.tencent.angel.ml.lda.psf;

import com.tencent.angel.PartitionKey;
import com.tencent.angel.ml.matrix.psf.get.base.*;
import com.tencent.angel.ml.matrix.psf.get.multi.PartitionGetRowsParam;
import com.tencent.angel.ps.impl.MatrixPartitionManager;
import com.tencent.angel.ps.impl.PSContext;
import com.tencent.angel.ps.impl.matrix.ServerRow;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.util.ArrayList;
import java.util.List;


public class GetPartFunc extends GetFunc {

  private final static Log LOG = LogFactory.getLog(GetPartFunc.class);

  public GetPartFunc(GetParam param) {
    super(param);
  }

  public GetPartFunc() { super(null);}

  @Override
  public PartitionGetResult partitionGet(PartitionGetParam partParam) {
    PartitionGetRowsParam param = (PartitionGetRowsParam) partParam;

    PartitionKey pkey = param.getPartKey();
    pkey = PSContext.get().getMatrixPartitionManager()
            .getPartition(pkey.getMatrixId(), pkey.getPartitionId())
            .getPartitionKey();
    int ws = pkey.getStartRow();
    int es = pkey.getEndRow();

    List<Integer> reqRows = param.getRowIndexes();

    MatrixPartitionManager manager = PSContext.get().getMatrixPartitionManager();
    List<ServerRow> rows = new ArrayList<>();
    for (int w : reqRows)
      rows.add(manager.getRow(pkey, w));

    PartCSRResult csr = new PartCSRResult(rows);
    return csr;
  }

  @Override
  public GetResult merge(List<PartitionGetResult> partResults) {
    return null;
  }
}
