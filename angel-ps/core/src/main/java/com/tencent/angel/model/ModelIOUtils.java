package com.tencent.angel.model;

import com.tencent.angel.ps.storage.partition.RowBasedPartition;
import java.util.ArrayList;
import java.util.List;

public class ModelIOUtils {
  public static List<Integer> filter(RowBasedPartition part, List<Integer> rowIds) {
    List<Integer> ret = new ArrayList<>();
    for (int rowId : rowIds) {
      if (part.hasRow(rowId)) {
        ret.add(rowId);
      }
    }

    return ret;
  }

}
