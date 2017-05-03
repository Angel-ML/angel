/*
 * Tencent is pleased to support the open source community by making Angel available.
 *
 * Copyright (C) 2017 THL A29 Limited, a Tencent company. All rights reserved.
 *
 * Licensed under the BSD 3-Clause License (the "License"); you may not use this file except in
 * compliance with the License. You may obtain a copy of the License at
 *
 * https://opensource.org/licenses/BSD-3-Clause
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is
 * distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied. See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.tencent.angel.ml.algorithm.utils;


import com.tencent.angel.ml.math.vector.SparseDoubleVector;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.DoubleBuffer;
import java.nio.IntBuffer;
import java.util.ArrayList;

public class PSModelLoader {
  private static final Log LOG = LogFactory.getLog(PSModelLoader.class);

  private static FileSystem infs;
  private static int maxEndCol = 0;
  private static int maxEndRow = 0;
  private static ArrayList<PartFile> partFiles = new ArrayList<PartFile>();

  static class PartFile {
    FileStatus status;
    int startrow;
    int endrow;
    int startcol;
    int endcol;
  }

  private static void loadDoubleVector(PartFile part, double[] resultArray) throws IOException {
    LOG.info("open file " + part.status.getPath());

    FSDataInputStream fin = infs.open(part.status.getPath());
    fin.readInt();
    fin.readInt();

    // read partition header
    int startRow = fin.readInt();
    int startCol = fin.readInt();
    int endRow = fin.readInt();
    int endCol = fin.readInt();
    String rowType = fin.readUTF();
    LOG.info("rowType " + rowType + ", partition range is [" + startRow + ", " + startCol + "] to ["
        + endRow + ", " + endCol + "]");

    int rowNum = fin.readInt();
    System.out.println("rowNum " + rowNum);

    switch (rowType) {
      case "T_DOUBLE_SPARSE": {
        for (int j = 0; j < rowNum; j++) {
          int rowIndex = fin.readInt();
          int rowLen = fin.readInt();
          int startPos = rowIndex * maxEndCol + startCol;
          for (int k = 0; k < rowLen; k++) {
            int colIndex = fin.readInt();
            resultArray[startPos + colIndex] = fin.readDouble();
          }
        }
        break;
      }

      case "T_DOUBLE_DENSE": {
        byte[] data = new byte[8 * (endCol - startCol)];
        for (int j = 0; j < rowNum; j++) {
          int rowIndex = fin.readInt();
          System.out.println("rowIndex: " + rowIndex);
          int rowLen = endCol - startCol;
          fin.read(data, 0, data.length);
          // DoubleBuffer dBuffer = ByteBuffer
          // .wrap(data, 0, data.length)
          // .order(ByteOrder.nativeOrder()).asDoubleBuffer();
          DoubleBuffer dBuffer = ByteBuffer.wrap(data, 0, data.length).asDoubleBuffer();

          int startPos = rowIndex * maxEndCol + startCol;

          for (int k = 0; k < rowLen; k++) {
            resultArray[startPos + k] = dBuffer.get();
            if (k < 10) {
              System.out.println("resultArray: " + resultArray[startPos + k]);
            }
          }
        }
        break;
      }

      case "T_INT_SPARSE": {
        for (int j = 0; j < rowNum; j++) {
          int rowIndex = fin.readInt();
          int rowLen = fin.readInt();
          int startPos = rowIndex * maxEndCol + startCol;
          for (int k = 0; k < rowLen; k++) {
            int colIndex = fin.readInt();
            resultArray[startPos + colIndex] = fin.readInt();
          }
        }
        break;
      }

      case "T_INT_DENSE": {
        byte[] data = new byte[4 * (endCol - startCol)];
        for (int j = 0; j < rowNum; j++) {
          int rowIndex = fin.readInt();
          int rowLen = endCol - startCol;
          fin.read(data, 0, data.length);
          IntBuffer iBuffer =
              ByteBuffer.wrap(data, 0, data.length).order(ByteOrder.nativeOrder()).asIntBuffer();

          int startPos = rowIndex * maxEndCol + startCol;

          for (int k = 0; k < rowLen; k++) {
            resultArray[startPos + k] = iBuffer.get();
          }

          for (int k = 0; k < rowLen; k++) {
            LOG.info("Array[" + String.valueOf(k) + ": " + String.valueOf(resultArray));
          }
        }
        break;
      }
    }
    fin.close();
  }

  public static void init(String inputStr, String modelName) throws IOException {
    LOG.info("init PSModelLoader form " + inputStr);
    Path inputPath = new Path(inputStr);
    Configuration conf = new Configuration();
    infs = inputPath.getFileSystem(conf);

    PathFilter psFilter = new PathFilter() {
      @Override
      public boolean accept(Path path) {
        return path.getName().contains("ParameterServer_");
      }
    };

    FileStatus[] psFolders;
    psFolders = infs.listStatus(inputPath, psFilter);

    if (psFolders.length == 0) {
      LOG.info("no validate filr in " + inputStr);
    }

    for (int i = 0; i < psFolders.length; i++) {
      LOG.info(psFolders[i].getPath().toString());
    }

    for (int i = 0; i < psFolders.length; i++) {
      Path modelPath = new Path(inputStr + "/ParameterServer_" + i + "/" + modelName);
      LOG.info("load vectors from " + modelPath.getName());
      FileStatus[] parts = infs.listStatus(modelPath);

      for (int j = 0; j < parts.length; j++) {
        PartFile part = new PartFile();
        part.status = parts[j];
        FSDataInputStream fin = infs.open(parts[j].getPath());

        int matrixId = fin.readInt();
        int partSize = fin.readInt();
        part.startrow = fin.readInt();
        part.startcol = fin.readInt();
        part.endrow = fin.readInt();
        part.endcol = fin.readInt();

        partFiles.add(part);

        if (maxEndCol < part.endcol) {
          maxEndCol = part.endcol;
        }

        if (maxEndRow < part.endrow) {
          maxEndRow = part.endrow;
        }

        fin.close();
      }
    }
    System.out
        .println(String.format("matrix row:[%d, %d], col:[%d, %d]", 0, maxEndRow, 0, maxEndCol));
  }

  public static SparseDoubleVector loadVector(String inputDir, String vectorName)
      throws IOException {
    init(inputDir, vectorName);
    double[] vectorArray = new double[maxEndCol * maxEndRow];
    for (int i = 0; i < partFiles.size(); i++) {
      PartFile part = partFiles.get(i);
      loadDoubleVector(part, vectorArray);
    }
    // DenseDoubleVector vector = new DenseDoubleVector(vectorArray.length, vectorArray);
    SparseDoubleVector vector = new SparseDoubleVector(vectorArray.length);
    for (int i = 0; i < vectorArray.length; i++) {
      if (vectorArray[i] != 0) {
        vector.set(i, vectorArray[i]);
      }
    }
    return vector;
  }

}
