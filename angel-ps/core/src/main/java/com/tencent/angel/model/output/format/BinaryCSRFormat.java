package com.tencent.angel.model.output.format;

import com.tencent.angel.ml.math2.matrix.Matrix;
import com.tencent.angel.model.MatrixLoadContext;
import com.tencent.angel.model.PSMatrixLoadContext;
import com.tencent.angel.model.PSMatrixSaveContext;
import com.tencent.angel.ps.storage.partition.CSRPartition;
import com.tencent.angel.ps.storage.partition.ServerPartition;
import com.tencent.angel.ps.storage.partition.storage.CSRStorage;
import com.tencent.angel.ps.storage.partition.storage.IntCSRStorage;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;

public class BinaryCSRFormat extends MatrixFormatImpl {

  public BinaryCSRFormat(Configuration conf) {
    super(conf);
  }

  @Override
  public void save(ServerPartition part, MatrixPartitionMeta partMeta,
      PSMatrixSaveContext saveContext, DataOutputStream output) throws IOException {
    CSRPartition csrPart = (CSRPartition) part;
    CSRStorage storage = csrPart.getStorage();

    if(storage instanceof IntCSRStorage) {
      save((IntCSRStorage) storage, output);
    }
  }

  public void save(IntCSRStorage storage, DataOutputStream output) throws IOException {
    int [] rowOffset = storage.getRowOffsets();
    int [] columnIndices = storage.getColumnIndices();
    int [] values = storage.getValues();
    output.writeInt(rowOffset.length);
    for(int i = 0; i < rowOffset.length; i++) {
      output.writeInt(rowOffset[i]);
    }

    output.writeInt(columnIndices.length);
    for(int i = 0; i < columnIndices.length; i++) {
      output.writeInt(columnIndices[i]);
    }

    if(values != null) {
      output.writeInt(values.length);
      for(int i = 0; i < values.length; i++) {
        output.writeInt(values[i]);
      }
    } else {
      output.writeInt(0);
    }
  }

  @Override
  public void load(ServerPartition part, MatrixPartitionMeta partMeta,
      PSMatrixLoadContext loadContext, DataInputStream input) throws IOException {
    CSRPartition csrPart = (CSRPartition) part;
    CSRStorage storage = csrPart.getStorage();
    if(storage instanceof IntCSRStorage) {
      load((IntCSRStorage) storage, input);
    }
  }

  private void load(IntCSRStorage storage, DataInputStream input) throws IOException {
    int [] rowOffsets = new int[input.readInt()];
    for(int i = 0; i < rowOffsets.length; i++) {
      rowOffsets[i] = input.readInt();
    }

    int [] columnIndices = new int[input.readInt()];
    for(int i = 0; i < columnIndices.length; i++) {
      columnIndices[i] = input.readInt();
    }

    int [] values = new int[input.readInt()];
    for(int i = 0; i < values.length; i++) {
      values[i] = input.readInt();
    }
  }

  @Override
  public void load(Matrix matrix, MatrixPartitionMeta partMeta, MatrixLoadContext loadContext,
      FSDataInputStream in) throws IOException {
    throw new UnsupportedOperationException("Does not support load CSR format local now");
  }
}
