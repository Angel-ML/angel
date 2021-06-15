package com.tencent.angel.ml.matrix;

import com.tencent.angel.PartitionKey;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;

public class MatrixMetaUtils {
  public static List<PartitionKey> getPartitions(MatrixMeta matrixMeta, int rowIndex) {
    List<PartitionKey> partitionKeys = new ArrayList<>();
    Iterator<PartitionMeta> iter = matrixMeta.getPartitionMetas().values().iterator();
    while (iter.hasNext()) {
      PartitionKey partitionKey = iter.next().getPartitionKey();
      if (partitionKey.getStartRow() <= rowIndex
          && partitionKey.getEndRow() > rowIndex)
        partitionKeys.add(partitionKey);
    }

    // Sort the partitions by start column index
    partitionKeys.sort(new Comparator<PartitionKey>() {
      @Override public int compare(PartitionKey p1, PartitionKey p2) {
        if (p1.getStartCol() < p2.getStartCol()) {
          return -1;
        } else if (p1.getStartCol() > p2.getStartCol()) {
          return 1;
        } else {
          return 0;
        }
      }
    });
    return partitionKeys;
  }

  public static List<PartitionKey> getPartitions(MatrixMeta matrixMeta) {
    List<PartitionKey> partitionKeys = new ArrayList<>();
    Iterator<PartitionMeta> iter = matrixMeta.getPartitionMetas().values().iterator();
    while (iter.hasNext()) {
      partitionKeys.add(iter.next().getPartitionKey());
    }
    partitionKeys.sort((PartitionKey p1, PartitionKey p2) -> {
      if (p1.getStartCol() < p2.getStartCol()) {
        return -1;
      } else if (p1.getStartCol() > p2.getStartCol()) {
        return 1;
      } else {
        return 0;
      }
    });
    return partitionKeys;
  }
}
