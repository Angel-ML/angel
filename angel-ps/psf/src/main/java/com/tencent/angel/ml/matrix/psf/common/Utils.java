/*
 * Tencent is pleased to support the open source community by making Angel available.
 *
 * Copyright (C) 2017-2018 THL A29 Limited, a Tencent company. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in 
 * compliance with the License. You may obtain a copy of the License at
 *
 * https://opensource.org/licenses/Apache-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 *
 */

package com.tencent.angel.ml.matrix.psf.common;

import com.tencent.angel.PartitionKey;
import com.tencent.angel.ps.impl.matrix.ServerRow;
import com.tencent.angel.ps.impl.matrix.ServerSparseDoubleLongKeyRow;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Map;

public class Utils {
  public static boolean withinPart(PartitionKey partKey, int[] rowIds) {
    int startRow = partKey.getStartRow();
    int endRow = partKey.getEndRow();

    boolean allInPart = true;
    boolean hasInPart = false;
    boolean hasOutPart = false;

    for (int i = 0; i < rowIds.length; i++) {
      if (rowIds[i] < startRow || rowIds[i] >= endRow) {
        allInPart = false;
        hasOutPart = true;
      } else {
        hasInPart = true;
      }
    }

    if (hasInPart && hasOutPart) {
      throw new RuntimeException("rowIds: " + Arrays.toString(rowIds) + " in different parts");
    }
    return allInPart;
  }

  public static int[] intListToArray(ArrayList<Integer> inArray) {
    int[] outArray = new int[inArray.size()];
    for (int i = 0; i < inArray.size(); i++) {
      outArray[i] = inArray.get(i);
    }
    return outArray;
  }

  public static long[] longListToArray(ArrayList<Long> inArray) {
    long[] outArray = new long[inArray.size()];
    for (int i = 0; i < inArray.size(); i++) {
      outArray[i] = inArray.get(i);
    }
    return outArray;
  }

  public static float[] floatListToArray(ArrayList<Float> inArray) {
    float[] outArray = new float[inArray.size()];
    for (int i = 0; i < inArray.size(); i++) {
      outArray[i] = inArray.get(i);
    }
    return outArray;
  }

  public static double[] doubleListToArray(ArrayList<Double> inArray) {
    double[] outArray = new double[inArray.size()];
    for (int i = 0; i < inArray.size(); i++) {
      outArray[i] = inArray.get(i);
    }
    return outArray;
  }

  public static ArrayList<Map.Entry<ServerRow, ArrayList<Integer>>> pick(ServerRow[] serverRows, Long[] rows, Long[] cols, Double[] values) {
    assert (rows.length == cols.length && rows.length == values.length);


    ArrayList<Map.Entry<ServerRow, ArrayList<Integer>>> result = new ArrayList<>();
    for (ServerRow row : serverRows) {
      long rowId = row.getRowId();
      long startCol = row.getStartCol();
      long endCol = row.getEndCol();

      ArrayList<Integer> indics = new ArrayList<Integer>();
      for (int i = 0; i <= rows.length; i++) {
        if (rowId == rows[i] && cols[i] >= startCol && cols[i] < endCol) {
          indics.add(i);
        }
      }
      Map.Entry<ServerRow, ArrayList<Integer>> pair=new java.util.AbstractMap.SimpleEntry<>(row, indics);
      result.add(pair);
    }

    return result;
  }

}
