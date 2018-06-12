package com.tencent.angel.ml.lda.psf;

import com.tencent.angel.PartitionKey;
import com.tencent.angel.ml.matrix.psf.get.base.GetParam;
import com.tencent.angel.ml.matrix.psf.get.base.PartitionGetParam;
import com.tencent.angel.ml.matrix.psf.get.multi.PartitionGetRowsParam;
import com.tencent.angel.psagent.PSAgentContext;

import java.util.ArrayList;
import java.util.List;

public class GetColumnParam extends GetParam {

  private final List<Integer> columns;

  public GetColumnParam(int matrixId, List<Integer> columns) {
    super(matrixId);
    this.columns = columns;
  }

  @Override
  public List<PartitionGetParam> split() {
    List<PartitionKey> pkeys = PSAgentContext.get()
            .getMatrixMetaManager().getPartitions(matrixId);

    List<PartitionGetParam> partParams = new ArrayList<PartitionGetParam>(pkeys.size());

    for (PartitionKey entry : pkeys) {
      partParams.add(new PartitionGetRowsParam(matrixId, entry, columns));
    }

    return partParams;
  }

}
