package com.tencent.angel.model.output.format;

import com.tencent.angel.ps.ParameterServerId;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Model ouput utils
 */
public class ModelFilesUtils {
  private static final ConcurrentHashMap<Integer, AtomicInteger> psModelFileGens = new ConcurrentHashMap<>();

  /**
   * Get a new output file name for ps model, file name format : psId_index
   * @param psId parameterserver id
   * @return a new file name
   */
  public static String nextFileName(ParameterServerId psId, int matrixId) {
    if(!psModelFileGens.containsKey(matrixId)) {
      psModelFileGens.putIfAbsent(matrixId, new AtomicInteger(0));
    }

    return psId + ModelFilesConstent.separator + psModelFileGens.get(matrixId).getAndIncrement();
  }

  /**
   * Get a output file name for ps model, file name format : psId_partid
   * @param psId parameterserver id
   * @param startPartId minimal partition id
   * @return
   */
  public static String fileName(ParameterServerId psId, int startPartId) {
    return psId + ModelFilesConstent.separator + startPartId;
  }
}
