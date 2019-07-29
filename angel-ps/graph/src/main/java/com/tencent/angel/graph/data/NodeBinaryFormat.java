package com.tencent.angel.graph.data;

import com.tencent.angel.ml.math2.matrix.Matrix;
import com.tencent.angel.model.MatrixLoadContext;
import com.tencent.angel.model.PSMatrixLoadContext;
import com.tencent.angel.model.PSMatrixSaveContext;
import com.tencent.angel.model.output.format.MatrixPartitionMeta;
import com.tencent.angel.model.output.format.RowBasedFormat;
import com.tencent.angel.ps.storage.partition.RowBasedPartition;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;

public class NodeBinaryFormat extends RowBasedFormat {

  public NodeBinaryFormat(Configuration conf) {
    super(conf);
  }

  @Override
  public void load(RowBasedPartition part, MatrixPartitionMeta partMeta,
      PSMatrixLoadContext loadContext, DataInputStream input) throws IOException {

  }

  @Override
  public void save(RowBasedPartition part, MatrixPartitionMeta partMeta,
      PSMatrixSaveContext saveContext, DataOutputStream output) throws IOException {

  }

  @Override
  public void load(Matrix matrix, MatrixPartitionMeta partMeta, MatrixLoadContext loadContext,
      FSDataInputStream in) throws IOException {

  }
}
