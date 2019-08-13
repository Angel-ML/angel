package com.tencent.angel.spark.ml.psf.pagerank;

import com.tencent.angel.ml.math2.vector.FloatVector;
import com.tencent.angel.ml.matrix.psf.get.base.GetFunc;
import com.tencent.angel.ml.matrix.psf.get.base.GetResult;
import com.tencent.angel.ml.matrix.psf.get.base.PartitionGetParam;
import com.tencent.angel.ml.matrix.psf.get.base.PartitionGetResult;
import com.tencent.angel.ps.storage.vector.ServerLongFloatRow;
import com.tencent.angel.ps.storage.vector.ServerRow;
import com.tencent.angel.ps.storage.vector.ServerRowUtils;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

public class MyPull extends GetFunc {

  public MyPull(int matrixId, int deltaId, int sumId,
                float resetProb, float tol, long[] keys) {
    super(new MyPullParam(matrixId, deltaId, sumId, resetProb, tol, keys));
  }

  public MyPull() {
    super(null);
  }

  @Override
  public PartitionGetResult partitionGet(PartitionGetParam partParam) {
    MyPullPartParam param = (MyPullPartParam) partParam;
    ServerRow msgRows = psContext.getMatrixStorageManager().getRow(param.getPartKey(), param.getDeltaId());
    ServerRow sumsRow = psContext.getMatrixStorageManager().getRow(param.getPartKey(), param.getSumId());
    long start = param.getPartKey().getStartCol();
    long range = param.getPartKey().getEndCol() - start;
    FloatVector msgs = ServerRowUtils.getVector((ServerLongFloatRow) msgRows);
    FloatVector sums = ServerRowUtils.getVector((ServerLongFloatRow) sumsRow);

    return new MyPullPartResult(param.getKeys(), start, msgs, sums, param.getResetProb(), param.getTol());
  }


  @Override
  public GetResult merge(List<PartitionGetResult> partResults) {
    List<MyPullPartResult> lists = new ArrayList<>(partResults.size());
    for (int i = 0; i < partResults.size(); i++)
      lists.add((MyPullPartResult) partResults.get(i));

    lists.sort(new Comparator<MyPullPartResult>() {
      @Override
      public int compare(MyPullPartResult o1, MyPullPartResult o2) {
        if (o1.getStart() == o2.getStart()) return 0;
        return o1.getStart() < o2.getStart() ? -1 : 1;
      }
    });

    int size = 0;
    for (MyPullPartResult result : lists)
      size += result.getKeys().length;

    long[] keys = new long[size];
    float[] vals = new float[size];
    int start = 0;
    for (MyPullPartResult result : lists) {
      System.arraycopy(result.getKeys(), 0, keys, start, result.getKeys().length);
      System.arraycopy(result.getValues(), 0, vals, start, result.getValues().length);
      start += result.getKeys().length;
    }

    return new MyPullResult(keys, vals);
  }
}
