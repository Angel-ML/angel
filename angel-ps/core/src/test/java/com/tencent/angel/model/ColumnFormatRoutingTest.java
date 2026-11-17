package com.tencent.angel.model;

import com.tencent.angel.PartitionKey;
import com.tencent.angel.ml.matrix.RowType;
import com.tencent.angel.psagent.matrix.transport.router.RouterType;
import com.tencent.angel.model.output.format.MatrixPartitionMeta;
import com.tencent.angel.model.output.format.TextColumnFormat;
import com.tencent.angel.ps.storage.partition.RowBasedPartition;
import com.tencent.angel.ps.storage.partition.storage.DenseServerRowsStorage;
import com.tencent.angel.ps.storage.vector.ServerIntDoubleRow;
import java.io.*;
import java.util.*;
import org.apache.hadoop.conf.Configuration;
import org.junit.Test;
import static org.junit.Assert.*;

public class ColumnFormatRoutingTest {
  @Test public void hashKeysAreNotShiftedByBucketId() throws Exception { roundTrip(RouterType.HASH, 2, 3, new int[]{5, 128, 255}); }
  @Test public void rangeKeysUseStorageOffset() throws Exception { roundTrip(RouterType.RANGE, 100, 256, new int[]{101, 128, 255}); }

  private void roundTrip(RouterType router, int start, int end, int[] keys) throws Exception {
    for (boolean sorted : new boolean[]{false, true}) {
      PartitionKey key = new PartitionKey(2, 0, 0, start, 1, end);
      DenseServerRowsStorage storage = new DenseServerRowsStorage(0, 1);
      ServerIntDoubleRow row = new ServerIntDoubleRow(0, RowType.T_DOUBLE_SPARSE, start, end, 4, router);
      row.init();
      storage.putRow(0, row);
      RowBasedPartition part = new RowBasedPartition(key, 4, storage, RowType.T_DOUBLE_SPARSE, null, router);
      for (int i = 0; i < keys.length; i++) row.set(keys[i], 10.5 + i);
      TextColumnFormat format = new TextColumnFormat(new Configuration(false));
      PSMatrixSaveContext context = new PSMatrixSaveContext(0, Arrays.asList(2), new ArrayList<>(Arrays.asList(0)), format.getClass().getName(), "", false, sorted);
      MatrixPartitionMeta meta = new MatrixPartitionMeta();
      ByteArrayOutputStream bytes = new ByteArrayOutputStream();
      format.save(part, meta, context, new DataOutputStream(bytes));
      String[] lines = bytes.toString("UTF-8").trim().split("\n");
      assertEquals(keys.length, lines.length);
      Set<Integer> found = new HashSet<>();
      for (String line : lines) {
        String[] fields = line.split(",");
        int column = Integer.parseInt(fields[0]);
        found.add(column);
        assertEquals(row.get(column), Double.parseDouble(fields[1]), 0);
      }
      for (int column : keys) assertTrue(found.contains(column));
      row.reset();
      format.load(part, meta, null, new DataInputStream(new ByteArrayInputStream(bytes.toByteArray())));
      for (int i = 0; i < keys.length; i++) assertEquals(10.5 + i, row.get(keys[i]), 0);
    }
  }
}
