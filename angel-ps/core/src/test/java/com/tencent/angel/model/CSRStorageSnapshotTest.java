package com.tencent.angel.model;

import com.tencent.angel.PartitionKey;
import com.tencent.angel.ml.matrix.RowType;
import com.tencent.angel.model.output.format.SnapshotFormat;
import com.tencent.angel.ps.storage.partition.CSRPartition;
import com.tencent.angel.ps.storage.partition.storage.IntCSRStorage;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import org.apache.hadoop.conf.Configuration;
import org.junit.Test;

import static org.junit.Assert.assertArrayEquals;

public class CSRStorageSnapshotTest {
  @Test public void roundTripsNeighborsAndValues() throws Exception {
    IntCSRStorage source = new IntCSRStorage(0);
    source.setRowOffsets(new int[]{0, 2, 3});
    source.setColumnIndices(new int[]{1, 2, 0});
    source.setValues(new int[]{4, 5, 6});
    IntCSRStorage target = new IntCSRStorage(0);
    SnapshotFormat format = new SnapshotFormat(new Configuration(false));
    CSRPartition partition = new CSRPartition(new PartitionKey(), 3, source, RowType.T_INT_SPARSE);
    CSRPartition restored = new CSRPartition(new PartitionKey(), 3, target, RowType.T_INT_SPARSE);
    ByteArrayOutputStream bytes = new ByteArrayOutputStream();
    format.save(partition, null, null, new DataOutputStream(bytes));
    format.load(restored, null, null, new DataInputStream(new ByteArrayInputStream(bytes.toByteArray())));
    assertArrayEquals(source.getRowOffsets(), target.getRowOffsets());
    assertArrayEquals(source.getColumnIndices(), target.getColumnIndices());
    assertArrayEquals(source.getValues(), target.getValues());
  }

  @Test public void supportsNeighborOnlyStorage() throws Exception {
    IntCSRStorage source = new IntCSRStorage(0);
    source.setRowOffsets(new int[]{0, 1});
    source.setColumnIndices(new int[]{1});
    ByteArrayOutputStream bytes = new ByteArrayOutputStream();
    source.serialize(new DataOutputStream(bytes));
    IntCSRStorage restored = new IntCSRStorage(0);
    restored.deserialize(new DataInputStream(new ByteArrayInputStream(bytes.toByteArray())));
    assertArrayEquals(source.getRowOffsets(), restored.getRowOffsets());
    assertArrayEquals(source.getColumnIndices(), restored.getColumnIndices());
    assertArrayEquals(new int[0], restored.getValues());
  }
}
