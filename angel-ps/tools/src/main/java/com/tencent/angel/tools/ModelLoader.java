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

package com.tencent.angel.tools;

import com.google.common.base.Preconditions;
import com.tencent.angel.ml.matrix.RowType;
import com.tencent.angel.model.output.format.ModelFilesConstent;
import com.tencent.angel.model.output.format.ModelFilesMeta;
import com.tencent.angel.model.output.format.ModelPartitionMeta;
import com.tencent.angel.model.output.format.ModelPartitionMeta.RowOffset;
import it.unimi.dsi.fastutil.ints.Int2DoubleOpenHashMap;
import it.unimi.dsi.fastutil.ints.Int2FloatOpenHashMap;
import it.unimi.dsi.fastutil.ints.Int2IntOpenHashMap;
import it.unimi.dsi.fastutil.longs.Long2DoubleOpenHashMap;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Vector;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.RecursiveAction;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

/**
 * Model load tools
 */
public class ModelLoader {

  private static final Log LOG = LogFactory.getLog(ModelLoader.class);
  private static volatile int batchNum = 1;
  private final static ForkJoinPool pool =
      new ForkJoinPool(Runtime.getRuntime().availableProcessors() * 2);

  /**
   * Read model row type
   *
   * @param modelDir model save directory
   * @return row type 0:sparse double, 1:dense double, 2:sparse int, 3:dense int, 4:dense float,
   * 5:sparse float, 7:sparse long key double
   */
  public static int getModelFormat(String modelDir) throws IOException {
    Configuration conf = new Configuration();
    Path meteFilePath = new Path(new Path(modelDir), "meta");

    FileSystem fs = meteFilePath.getFileSystem(conf);
    if (!fs.exists(meteFilePath)) {
      throw new IOException("matrix meta file does not exist ");
    }

    FSDataInputStream input = fs.open(meteFilePath);

    try {
      input.readInt();
      input.readUTF();
      return input.readInt();
    } finally {
      input.close();
    }
  }

  /**
   * Base class of model
   */
  static abstract class Model {

    /**
     * Row type
     */
    RowType rowType;

    /**
     * Row number
     */
    int row;

    /**
     * Column number
     */
    long col;

    /**
     * Create a model
     *
     * @param rowType row type
     * @param row row number
     * @param col column number
     */
    public Model(RowType rowType, int row, long col) {
      this.rowType = rowType;
      this.row = row;
      this.col = col;
    }

    /**
     * Get row type
     */
    RowType getRowType() {
      return rowType;
    }
  }


  /**
   * Dense double model, it use a 2-dimension double array to store model data
   */
  static class DenseDoubleModel extends Model {

    /**
     * model data
     */
    double[][] model;

    /**
     * Create a dense double model
     *
     * @param row row number
     * @param col column number
     */
    public DenseDoubleModel(int row, long col) {
      super(RowType.T_DOUBLE_DENSE, row, col);
      model = new double[row][];
    }

    /**
     * Get a model row
     *
     * @param rowId row index
     * @return model row
     */
    public double[] getRow(int rowId) {
      synchronized (this) {
        if (model[rowId] == null) {
          model[rowId] = new double[(int) col];
        }
        return model[rowId];
      }
    }

    /**
     * Get whole model data
     *
     * @return whole model data
     */
    public double[][] getModel() {
      return model;
    }
  }


  /**
   * Sparse double model, it use (int->double) maps to store model data and each map stores a row.
   */
  static class SparseDoubleModel extends Model {

    /**
     * Model data
     */
    Int2DoubleOpenHashMap[] model;

    /**
     * Row index -> (partId->values) map
     */
    Map<Integer, Map<Integer, Int2DoubleOpenHashMap>> tempModel;

    /**
     * Create a sparse double model
     *
     * @param row row number
     * @param col column number
     */
    public SparseDoubleModel(int row, long col) {
      super(RowType.T_DOUBLE_SPARSE, row, col);
      tempModel = new HashMap<>();
    }

    /**
     * Get a model row use row index
     *
     * @param rowId row index
     * @param partId partition index
     * @return a model row
     */
    public Int2DoubleOpenHashMap getRow(int rowId, int partId) {
      synchronized (this) {
        if (tempModel.get(rowId) == null) {
          tempModel.put(rowId, new HashMap<>());
          tempModel.get(rowId).put(partId, new Int2DoubleOpenHashMap());
        } else {
          if (tempModel.get(rowId).get(partId) == null) {
            tempModel.get(rowId).put(partId, new Int2DoubleOpenHashMap());
          }
        }

        return tempModel.get(rowId).get(partId);
      }
    }

    /**
     * Get a model row
     *
     * @param rowId row index
     * @param partId partition index
     * @param nnz estimated non-zero elements number
     * @return a model row
     */
    public Int2DoubleOpenHashMap getRow(int rowId, int partId, int nnz) {
      synchronized (this) {
        if (tempModel.get(rowId) == null) {
          tempModel.put(rowId, new HashMap<>());
          tempModel.get(rowId).put(partId, new Int2DoubleOpenHashMap(nnz));
        } else {
          if (tempModel.get(rowId).get(partId) == null) {
            tempModel.get(rowId).put(partId, new Int2DoubleOpenHashMap(nnz));
          }
        }

        return tempModel.get(rowId).get(partId);
      }
    }

    /**
     * Get the whole model
     *
     * @return the whole model
     */
    public Int2DoubleOpenHashMap[] getModel() {
      synchronized (this) {
        if (model == null) {
          model = new Int2DoubleOpenHashMap[row];
          for (Map.Entry<Integer, Map<Integer, Int2DoubleOpenHashMap>> rowEntry : tempModel
              .entrySet()) {
            int size = 0;
            for (Map.Entry<Integer, Int2DoubleOpenHashMap> partEntry : rowEntry.getValue()
                .entrySet()) {
              size += partEntry.getValue().size();
            }

            model[rowEntry.getKey()] = new Int2DoubleOpenHashMap(size);
            for (Map.Entry<Integer, Int2DoubleOpenHashMap> partEntry : rowEntry.getValue()
                .entrySet()) {
              model[rowEntry.getKey()].putAll(partEntry.getValue());
            }
          }
        }

        return model;
      }
    }
  }


  /**
   * Dense float model, it use a 2-dimension float array to store model
   */
  static class DenseFloatModel extends Model {

    /**
     * Model data
     */
    float[][] model;

    /**
     * Create a DenseFloatModel
     *
     * @param row row number
     * @param col column number
     */
    public DenseFloatModel(int row, long col) {
      super(RowType.T_FLOAT_DENSE, row, col);
      model = new float[row][];
    }

    /**
     * Get a model row
     *
     * @param rowId row index
     * @return model row
     */
    public float[] getRow(int rowId) {
      synchronized (this) {
        if (model[rowId] == null) {
          model[rowId] = new float[(int) col];
        }
        return model[rowId];
      }
    }

    /**
     * Get the whole model
     *
     * @return whole model
     */
    public float[][] getModel() {
      return model;
    }
  }


  /**
   * Sparse float model, it use (int->float) maps to store model data and each map stores a row.
   */
  static class SparseFloatModel extends Model {

    /**
     * model data
     */
    Int2FloatOpenHashMap[] model;

    /**
     * Row index -> (partId->values) map
     */
    Map<Integer, Map<Integer, Int2FloatOpenHashMap>> tempModel;

    /**
     * Create a SparseFloatModel
     *
     * @param row row number
     * @param col column number
     */
    public SparseFloatModel(int row, long col) {
      super(RowType.T_FLOAT_SPARSE, row, col);
      tempModel = new HashMap<>();
    }

    /**
     * Get a model row
     *
     * @param rowId row index
     * @param partId partition index
     * @return model row
     */
    public Int2FloatOpenHashMap getRow(int rowId, int partId) {
      synchronized (this) {
        if (tempModel.get(rowId) == null) {
          tempModel.put(rowId, new HashMap<>());
          tempModel.get(rowId).put(partId, new Int2FloatOpenHashMap());
        } else {
          if (tempModel.get(rowId).get(partId) == null) {
            tempModel.get(rowId).put(partId, new Int2FloatOpenHashMap());
          }
        }

        return tempModel.get(rowId).get(partId);
      }
    }

    /**
     * Get a model row
     *
     * @param rowId row index
     * @param partId partition index
     * @param nnz estimated non-zero elements number
     * @return model row
     */
    public Int2FloatOpenHashMap getRow(int rowId, int partId, int nnz) {
      synchronized (this) {
        if (tempModel.get(rowId) == null) {
          tempModel.put(rowId, new HashMap<>());
          tempModel.get(rowId).put(partId, new Int2FloatOpenHashMap(nnz));
        } else {
          if (tempModel.get(rowId).get(partId) == null) {
            tempModel.get(rowId).put(partId, new Int2FloatOpenHashMap(nnz));
          }
        }

        return tempModel.get(rowId).get(partId);
      }
    }

    /**
     * Get the whole model
     *
     * @return the whole model
     */
    public Int2FloatOpenHashMap[] getModel() {
      synchronized (this) {
        if (model == null) {
          model = new Int2FloatOpenHashMap[row];
          for (Map.Entry<Integer, Map<Integer, Int2FloatOpenHashMap>> rowEntry : tempModel
              .entrySet()) {
            int size = 0;
            for (Map.Entry<Integer, Int2FloatOpenHashMap> partEntry : rowEntry.getValue()
                .entrySet()) {
              size += partEntry.getValue().size();
            }

            model[rowEntry.getKey()] = new Int2FloatOpenHashMap(size);
            for (Map.Entry<Integer, Int2FloatOpenHashMap> partEntry : rowEntry.getValue()
                .entrySet()) {
              model[rowEntry.getKey()].putAll(partEntry.getValue());
            }
          }
        }

        return model;
      }
    }
  }


  /**
   * Dense int model, it use a 2-dimension int array to store model data
   */
  static class DenseIntModel extends Model {

    /**
     * model data
     */
    int[][] model;

    /**
     * Create a DenseIntModel
     *
     * @param row row number
     * @param col column number
     */
    public DenseIntModel(int row, long col) {
      super(RowType.T_INT_DENSE, row, col);
      model = new int[row][];
    }

    /**
     * Get a model row
     *
     * @param rowId row index
     * @return model row
     */
    public int[] getRow(int rowId) {
      synchronized (this) {
        if (model[rowId] == null) {
          model[rowId] = new int[(int) col];
        }
        return model[rowId];
      }
    }

    /**
     * Get the whole model
     */
    public int[][] getModel() {
      return model;
    }
  }


  /**
   * Sparse int model, it use (int->int) maps to store model data and each map stores a row.
   */
  static class SparseIntModel extends Model {

    /**
     * model data
     */
    Int2IntOpenHashMap[] model;

    /**
     * Row index -> (partId->values) map
     */
    Map<Integer, Map<Integer, Int2IntOpenHashMap>> tempModel;

    /**
     * Create a SparseIntModel
     *
     * @param row row number
     * @param col column number
     */
    public SparseIntModel(int row, long col) {
      super(RowType.T_INT_SPARSE, row, col);
      tempModel = new HashMap<>();
    }

    /**
     * Get a model row
     *
     * @param rowId row index
     * @param partId partition index
     * @return model row
     */
    public Int2IntOpenHashMap getRow(int rowId, int partId) {
      synchronized (this) {
        if (tempModel.get(rowId) == null) {
          tempModel.put(rowId, new HashMap<>());
          tempModel.get(rowId).put(partId, new Int2IntOpenHashMap());
        } else {
          if (tempModel.get(rowId).get(partId) == null) {
            tempModel.get(rowId).put(partId, new Int2IntOpenHashMap());
          }
        }

        return tempModel.get(rowId).get(partId);
      }
    }

    /**
     * Get a model row
     *
     * @param rowId row index
     * @param partId partition index
     * @param nnz estimated non-zero number zero number
     * @return model row
     */
    public Int2IntOpenHashMap getRow(int rowId, int partId, int nnz) {
      synchronized (this) {
        if (tempModel.get(rowId) == null) {
          tempModel.put(rowId, new HashMap<>());
          tempModel.get(rowId).put(partId, new Int2IntOpenHashMap(nnz));
        } else {
          if (tempModel.get(rowId).get(partId) == null) {
            tempModel.get(rowId).put(partId, new Int2IntOpenHashMap(nnz));
          }
        }

        return tempModel.get(rowId).get(partId);
      }
    }

    /**
     * Get the whole model
     *
     * @return the whole model
     */
    public Int2IntOpenHashMap[] getModel() {
      synchronized (this) {
        if (model == null) {
          model = new Int2IntOpenHashMap[row];
          for (Map.Entry<Integer, Map<Integer, Int2IntOpenHashMap>> rowEntry : tempModel
              .entrySet()) {
            int size = 0;
            for (Map.Entry<Integer, Int2IntOpenHashMap> partEntry : rowEntry.getValue()
                .entrySet()) {
              size += partEntry.getValue().size();
            }

            model[rowEntry.getKey()] = new Int2IntOpenHashMap(size);
            for (Map.Entry<Integer, Int2IntOpenHashMap> partEntry : rowEntry.getValue()
                .entrySet()) {
              model[rowEntry.getKey()].putAll(partEntry.getValue());
            }
          }
        }

        return model;
      }
    }
  }


  /**
   * Sparse double with long key model, it use (long->double) maps to store model data and each map
   * stores a row.
   */
  static class SparseDoubleLongKeyModel extends Model {

    /**
     * model data
     */
    Long2DoubleOpenHashMap[] model;

    /**
     * Row index -> (partId->values) map
     */
    Map<Integer, Map<Integer, Long2DoubleOpenHashMap>> tempModel;

    /**
     * Create a SparseDoubleLongKeyModel
     *
     * @param row row number
     * @param col column number
     */
    public SparseDoubleLongKeyModel(int row, long col) {
      super(RowType.T_DOUBLE_SPARSE_LONGKEY, row, col);
      tempModel = new HashMap<>();
    }

    /**
     * Get a model row
     *
     * @param rowId row index
     * @param partId partition index
     * @return model row
     */
    public Long2DoubleOpenHashMap getRow(int rowId, int partId) {
      synchronized (this) {
        if (tempModel.get(rowId) == null) {
          tempModel.put(rowId, new HashMap<>());
          tempModel.get(rowId).put(partId, new Long2DoubleOpenHashMap());
        } else {
          if (tempModel.get(rowId).get(partId) == null) {
            tempModel.get(rowId).put(partId, new Long2DoubleOpenHashMap());
          }
        }

        return tempModel.get(rowId).get(partId);
      }
    }

    /**
     * Get a model row
     *
     * @param rowId row index
     * @param partId partition index
     * @param nnz estimated non-zero elements number
     * @return model row
     */
    public Long2DoubleOpenHashMap getRow(int rowId, int partId, int nnz) {
      synchronized (this) {
        if (tempModel.get(rowId) == null) {
          tempModel.put(rowId, new HashMap<>());
          tempModel.get(rowId).put(partId, new Long2DoubleOpenHashMap(nnz));
        } else {
          if (tempModel.get(rowId).get(partId) == null) {
            tempModel.get(rowId).put(partId, new Long2DoubleOpenHashMap(nnz));
          }
        }

        return tempModel.get(rowId).get(partId);
      }
    }

    /**
     * Get the whole model
     *
     * @return the whole model
     */
    public Long2DoubleOpenHashMap[] getModel() {
      synchronized (this) {
        if (model == null) {
          model = new Long2DoubleOpenHashMap[row];
          for (Map.Entry<Integer, Map<Integer, Long2DoubleOpenHashMap>> rowEntry : tempModel
              .entrySet()) {
            int size = 0;
            for (Map.Entry<Integer, Long2DoubleOpenHashMap> partEntry : rowEntry.getValue()
                .entrySet()) {
              size += partEntry.getValue().size();
            }

            model[rowEntry.getKey()] = new Long2DoubleOpenHashMap(size);
            for (Map.Entry<Integer, Long2DoubleOpenHashMap> partEntry : rowEntry.getValue()
                .entrySet()) {
              model[rowEntry.getKey()].putAll(partEntry.getValue());
            }
          }
        }

        return model;
      }
    }
  }


  /**
   * Load operation
   */
  static class LoadOp extends RecursiveAction {

    /**
     * Model data
     */
    private final Model model;
    /**
     * The model save directory path
     */
    private final Path matrixPath;
    /**
     * File system handler
     */
    private final FileSystem fs;

    /**
     * Need load partitions list
     */
    private final List<Integer> partitionIds;
    /**
     * Model meta
     */
    private final ModelFilesMeta meta;
    /**
     * Error logs
     */
    private final Vector<String> errorMsgs;
    /**
     * ForkJoin start position
     */
    private final int startPos;
    /**
     * ForkJoin end position
     */
    private final int endPos;

    /**
     * Create a LoadOp
     *
     * @param model Model data
     * @param fs File system handler
     * @param matrixPath The model save directory path
     * @param partitionIds Need load partitions list
     * @param meta Model meta
     * @param errorMsgs Error logs
     * @param startPos ForkJoin start position
     * @param endPos ForkJoin end position
     */
    public LoadOp(Model model, FileSystem fs, Path matrixPath, List<Integer> partitionIds,
        ModelFilesMeta meta, Vector<String> errorMsgs, int startPos, int endPos) {
      this.model = model;
      this.fs = fs;
      this.matrixPath = matrixPath;
      this.partitionIds = partitionIds;
      this.meta = meta;
      this.errorMsgs = errorMsgs;
      this.startPos = startPos;
      this.endPos = endPos;
    }

    @Override
    protected void compute() {
      if (endPos <= startPos) {
        return;
      }

      if (endPos - startPos <= batchNum) {
        try {
          loadPartitions(model, matrixPath, fs, partitionIds, startPos, endPos, meta);
        } catch (Throwable x) {
          LOG.error("load model partitions failed.", x);
          errorMsgs.add("load model partitions failed." + x.getMessage());
        }
      } else {
        int middle = (startPos + endPos) / 2;
        LoadOp opLeft =
            new LoadOp(model, fs, matrixPath, partitionIds, meta, errorMsgs, startPos, middle);
        LoadOp opRight =
            new LoadOp(model, fs, matrixPath, partitionIds, meta, errorMsgs, middle, endPos);
        invokeAll(opLeft, opRight);
      }
    }
  }

  private static void loadPartitions(Model model, Path matrixPath, FileSystem fs,
      List<Integer> partitionIds, int startPos, int endPos, ModelFilesMeta meta)
      throws IOException {

    FSDataInputStream input = null;
    long offset = 0;
    String currentFileName = "";
    for (int i = startPos; i < endPos; i++) {
      ModelPartitionMeta partMeta = meta.getPartMeta(partitionIds.get(i));
      String fileName = partMeta.getFileName();
      offset = partMeta.getOffset();
      if (!fileName.equals(currentFileName)) {
        currentFileName = fileName;
        if (input != null) {
          input.close();
        }
        input = fs.open(new Path(matrixPath, currentFileName));

      }
      input.seek(offset);
      loadPartition(model, input, partMeta);
    }

    if (input != null) {
      input.close();
    }
  }

  private static void loadPartition(Model model, FSDataInputStream input,
      ModelPartitionMeta partMeta) throws IOException {
    switch (model.getRowType()) {
      case T_DOUBLE_SPARSE:
        loadSparseDoublePartition((SparseDoubleModel) model, input, partMeta);
        break;

      case T_DOUBLE_DENSE:
        loadDenseDoublePartition((DenseDoubleModel) model, input, partMeta);
        break;

      case T_INT_SPARSE:
        loadSparseIntPartition((SparseIntModel) model, input, partMeta);
        break;

      case T_INT_DENSE:
        loadDenseIntPartition((DenseIntModel) model, input, partMeta);
        break;

      case T_FLOAT_DENSE:
        loadDenseFloatPartition((DenseFloatModel) model, input, partMeta);
        break;

      case T_FLOAT_SPARSE:
        loadSparseFloatPartition((SparseFloatModel) model, input, partMeta);
        break;

      case T_DOUBLE_SPARSE_LONGKEY:
        loadSparseDoubleLongKeyPartition((SparseDoubleLongKeyModel) model, input, partMeta);
        break;

      default:
        throw new IOException("unsupport row type");
    }
  }

  private static void loadDenseDoublePartition(DenseDoubleModel model, FSDataInputStream input,
      ModelPartitionMeta partMeta) throws IOException {
    int rowNum = input.readInt();
    int startCol = (int) partMeta.getStartCol();
    int endCol = (int) partMeta.getEndCol();
    int rowId = 0;
    double[] row = null;
    for (int i = 0; i < rowNum; i++) {
      rowId = input.readInt();
      row = model.getRow(rowId);
      for (int j = startCol; j < endCol; j++) {
        row[j] = input.readDouble();
      }
    }
  }

  public static double[] loadDenseDoubleRowFromPartition(FSDataInputStream input,
      ModelPartitionMeta partMeta, int rowId)
      throws IOException {
    RowOffset rowOffset = partMeta.getRowMetas().get(rowId);
    input.seek(rowOffset.getOffset());
    Preconditions.checkState (input.readInt() == rowId);
    int num = (int) (partMeta.getEndCol() - partMeta.getStartCol());
    double[] row = new double[num];
    for (int i = 0; i < num; i++) {
      row[i] = input.readDouble();
    }
    return row;
  }

  private static void loadSparseDoublePartition(SparseDoubleModel model, FSDataInputStream input,
      ModelPartitionMeta partMeta) throws IOException {
    int rowNum = input.readInt();
    int rowId = 0;
    int nnz = 0;
    int totalNNZ = 0;
    Int2DoubleOpenHashMap row = null;
    for (int i = 0; i < rowNum; i++) {
      rowId = input.readInt();
      nnz = input.readInt();
      totalNNZ = (int) (nnz * (model.col) / (partMeta.getEndCol() - partMeta.getStartCol()));
      row = model.getRow(rowId, partMeta.getPartId(), totalNNZ);
      for (int j = 0; j < nnz; j++) {
        row.put(input.readInt(), input.readDouble());
      }
    }
  }

  public static Int2DoubleOpenHashMap loadSparseDoubleRowFromPartition(FSDataInputStream input,
      ModelPartitionMeta partMeta, int rowId) throws IOException {
    RowOffset rowOffset = partMeta.getRowMetas().get(rowId);
    input.seek(rowOffset.getOffset());
    Preconditions.checkState (input.readInt() == rowId);
    int num = input.readInt();
    Int2DoubleOpenHashMap row = new Int2DoubleOpenHashMap();
    for (int i = 0; i < num; i++) {
      row.put(input.readInt(), input.readDouble());
    }
    return row;
  }

  private static void loadDenseFloatPartition(DenseFloatModel model, FSDataInputStream input,
      ModelPartitionMeta partMeta) throws IOException {
    int rowNum = input.readInt();
    int startCol = (int) partMeta.getStartCol();
    int endCol = (int) partMeta.getEndCol();
    int rowId = 0;
    float[] row = null;
    for (int i = 0; i < rowNum; i++) {
      rowId = input.readInt();
      row = model.getRow(rowId);
      for (int j = startCol; j < endCol; j++) {
        row[j] = input.readFloat();
      }
    }
  }

  public static float[] loadDenseFloatRowFromPartition(FSDataInputStream input,
      ModelPartitionMeta partMeta, int rowId)
      throws IOException {
    RowOffset rowOffset = partMeta.getRowMetas().get(rowId);
    input.seek(rowOffset.getOffset());
    Preconditions.checkState (input.readInt() == rowId);
    int num = (int) (partMeta.getEndCol() - partMeta.getStartCol());
    float[] row = new float[num];
    for (int i = 0; i < num; i++) {
      row[i] = input.readFloat();
    }
    return row;
  }

  private static void loadSparseFloatPartition(SparseFloatModel model, FSDataInputStream input,
      ModelPartitionMeta partMeta) throws IOException {
    int rowNum = input.readInt();
    int rowId = 0;
    int nnz = 0;
    int totalNNZ = 0;
    Int2FloatOpenHashMap row = null;
    for (int i = 0; i < rowNum; i++) {
      rowId = input.readInt();
      nnz = input.readInt();
      totalNNZ = (int) (nnz * (model.col) / (partMeta.getEndCol() - partMeta.getStartCol()));
      row = model.getRow(rowId, partMeta.getPartId(), totalNNZ);
      for (int j = 0; j < nnz; j++) {
        row.put(input.readInt(), input.readFloat());
      }
    }
  }

  public static Int2FloatOpenHashMap loadSparseFloatRowFromPartition(FSDataInputStream input,
      ModelPartitionMeta partMeta, int rowId) throws IOException {
    RowOffset rowOffset = partMeta.getRowMetas().get(rowId);
    input.seek(rowOffset.getOffset());
    Preconditions.checkState (input.readInt() == rowId);
    int num = input.readInt();
    Int2FloatOpenHashMap row = new Int2FloatOpenHashMap();
    for (int i = 0; i < num; i++) {
      row.put(input.readInt(), input.readFloat());
    }
    return row;
  }

  private static void loadDenseIntPartition(DenseIntModel model, FSDataInputStream input,
      ModelPartitionMeta partMeta) throws IOException {
    int rowNum = input.readInt();
    int startCol = (int) partMeta.getStartCol();
    int endCol = (int) partMeta.getEndCol();
    int rowId = 0;
    int[] row = null;
    for (int i = 0; i < rowNum; i++) {
      rowId = input.readInt();
      row = model.getRow(rowId);
      for (int j = startCol; j < endCol; j++) {
        row[j] = input.readInt();
      }
    }
  }

  public static int[] loadDenseIntRowFromPartition(FSDataInputStream input,
      ModelPartitionMeta partMeta, int rowId)
      throws IOException {
    RowOffset rowOffset = partMeta.getRowMetas().get(rowId);
    input.seek(rowOffset.getOffset());
    Preconditions.checkState (input.readInt() == rowId);
    int num = (int) (partMeta.getEndCol() - partMeta.getStartCol());
    int[] row = new int[num];
    for (int i = 0; i < num; i++) {
      row[i] = input.readInt();
    }
    return row;
  }

  private static void loadSparseIntPartition(SparseIntModel model, FSDataInputStream input,
      ModelPartitionMeta partMeta) throws IOException {
    int rowNum = input.readInt();
    int rowId = 0;
    int nnz = 0;
    int totalNNZ = 0;
    Int2IntOpenHashMap row = null;

    for (int i = 0; i < rowNum; i++) {
      rowId = input.readInt();
      nnz = input.readInt();
      totalNNZ = (int) (nnz * (model.col) / (partMeta.getEndCol() - partMeta.getStartCol()));
      row = model.getRow(rowId, partMeta.getPartId(), totalNNZ);
      for (int j = 0; j < nnz; j++) {
        row.put(input.readInt(), input.readInt());
      }
    }
  }

  public static Int2IntOpenHashMap loadSparseIntRowFromPartition(FSDataInputStream input,
      ModelPartitionMeta partMeta, int rowId) throws IOException {
    RowOffset rowOffset = partMeta.getRowMetas().get(rowId);
    input.seek(rowOffset.getOffset());
    Preconditions.checkState (input.readInt() == rowId);
    int num = input.readInt();
    Int2IntOpenHashMap row = new Int2IntOpenHashMap();
    for (int i = 0; i < num; i++) {
      row.put(input.readInt(), input.readInt());
    }
    return row;
  }

  private static void loadSparseDoubleLongKeyPartition(SparseDoubleLongKeyModel model,
      FSDataInputStream input, ModelPartitionMeta partMeta) throws IOException {
    int rowNum = input.readInt();
    int rowId = 0;
    int nnz = 0;
    int totalNNZ = 0;
    Long2DoubleOpenHashMap row = null;

    for (int i = 0; i < rowNum; i++) {
      rowId = input.readInt();
      nnz = input.readInt();
      totalNNZ = (int) (nnz * (model.col) / (partMeta.getEndCol() - partMeta.getStartCol()));
      row = model.getRow(rowId, partMeta.getPartId(), totalNNZ);
      for (int j = 0; j < nnz; j++) {
        row.put(input.readLong(), input.readDouble());
      }
    }
  }

  public static Long2DoubleOpenHashMap loadSparseDoubleLongKeyRowFromPartition(
      FSDataInputStream input, ModelPartitionMeta partMeta, int rowId) throws IOException {
    RowOffset rowOffset = partMeta.getRowMetas().get(rowId);
    input.seek(rowOffset.getOffset());
    Preconditions.checkState(input.readInt() == rowId);
    int num = input.readInt();
    Long2DoubleOpenHashMap row = new Long2DoubleOpenHashMap();
    for (int j = 0; j < num; j++) {
      row.put(input.readLong(), input.readDouble());
    }
    return row;
  }


  /**
   * Get model meta
   *
   * @param modelDir model save directory path
   * @return model meta
   */
  public static ModelFilesMeta getMeta(String modelDir, Configuration conf) throws IOException {
    Path modelPath = new Path(modelDir);
    Path meteFilePath = new Path(modelPath, ModelFilesConstent.modelMetaFileName);
    ModelFilesMeta meta = new ModelFilesMeta();
    FileSystem fs = meteFilePath.getFileSystem(conf);
    if (!fs.exists(meteFilePath)) {
      throw new IOException("matrix meta file does not exist ");
    }
    FSDataInputStream input = fs.open(meteFilePath);
    meta.read(input);
    input.close();
    return meta;
  }

  /**
   * Load dense double model to a 2-dimension double array
   *
   * @param modelDir model save directory path
   * @return model data
   */
  public static double[][] loadToDoubleArrays(String modelDir, Configuration conf)
      throws IOException {
    // Load model meta
    ModelFilesMeta meta = getMeta(modelDir, conf);
    RowType rowType = RowType.valueOf(meta.getRowType());

    // Check row type
    if (rowType != RowType.T_DOUBLE_DENSE) {
      throw new IOException("model row type is not dense double, you should check it");
    }

    // Load model
    DenseDoubleModel model = new DenseDoubleModel(meta.getRow(), meta.getCol());
    loadModel(modelDir, model, meta, conf);

    return model.getModel();
  }

  /**
   * Load dense double model to int->double maps
   *
   * @param modelDir model save directory path
   * @return model data
   */
  public static Int2DoubleOpenHashMap[] loadToDoubleMaps(String modelDir, Configuration conf)
      throws IOException {
    // Load model meta
    ModelFilesMeta meta = getMeta(modelDir, conf);
    RowType rowType = RowType.valueOf(meta.getRowType());

    // Check row type
    if (rowType != RowType.T_DOUBLE_SPARSE && rowType != RowType.T_DOUBLE_SPARSE_COMPONENT) {
      throw new IOException("model row type is not sparse double, you should check it");
    }

    // Load model
    SparseDoubleModel model = new SparseDoubleModel(meta.getRow(), meta.getCol());
    loadModel(modelDir, model, meta, conf);

    return model.getModel();
  }

  /**
   * Load dense float model to a 2-dimension float array
   *
   * @param modelDir model save directory path
   * @return model data
   */
  public static float[][] loadToFloatArrays(String modelDir, Configuration conf)
      throws IOException {
    // Load model meta
    ModelFilesMeta meta = getMeta(modelDir, conf);
    RowType rowType = RowType.valueOf(meta.getRowType());

    // Check row type
    if (rowType != RowType.T_FLOAT_DENSE) {
      throw new IOException("model row type is not dense float, you should check it");
    }

    // Load model
    DenseFloatModel model = new DenseFloatModel(meta.getRow(), meta.getCol());
    loadModel(modelDir, model, meta, conf);

    return model.getModel();
  }

  /**
   * Load dense double model to int->float maps
   *
   * @param modelDir model save directory path
   * @return model data
   */
  public static Int2FloatOpenHashMap[] loadToFloatMaps(String modelDir, Configuration conf)
      throws IOException {
    // Load model meta
    ModelFilesMeta meta = getMeta(modelDir, conf);
    RowType rowType = RowType.valueOf(meta.getRowType());

    // Check row type
    if (rowType != RowType.T_FLOAT_SPARSE && rowType != rowType.T_FLOAT_SPARSE_COMPONENT) {
      throw new IOException("model row type is not sparse float, you should check it");
    }

    // Load model
    SparseFloatModel model = new SparseFloatModel(meta.getRow(), meta.getCol());
    loadModel(modelDir, model, meta, conf);

    return model.getModel();
  }

  /**
   * Load dense float model to a 2-dimension int array
   *
   * @param modelDir model save directory path
   * @return model data
   */
  public static int[][] loadToIntArrays(String modelDir, Configuration conf) throws IOException {
    // Load model meta
    ModelFilesMeta meta = getMeta(modelDir, conf);
    RowType rowType = RowType.valueOf(meta.getRowType());

    // Check row type
    if (rowType != RowType.T_INT_DENSE) {
      throw new IOException("model row type is not dense int, you should check it");
    }

    // Load model
    DenseIntModel model = new DenseIntModel(meta.getRow(), meta.getCol());
    loadModel(modelDir, model, meta, conf);

    return model.getModel();
  }

  /**
   * Load dense double model to int->int maps
   *
   * @param modelDir model save directory path
   * @return model data
   */
  public static Int2IntOpenHashMap[] loadToIntMaps(String modelDir, Configuration conf)
      throws IOException {
    // Load model meta
    ModelFilesMeta meta = getMeta(modelDir, conf);
    RowType rowType = RowType.valueOf(meta.getRowType());

    // Check row type
    if (rowType != RowType.T_INT_SPARSE && rowType != RowType.T_INT_SPARSE_COMPONENT) {
      throw new IOException("model row type is not sparse int, you should check it");
    }

    // Load model
    SparseIntModel model = new SparseIntModel(meta.getRow(), meta.getCol());
    loadModel(modelDir, model, meta, conf);

    return model.getModel();
  }

  /**
   * Load dense double model to long->double maps
   *
   * @param modelDir model save directory path
   * @return model data
   */
  public static Long2DoubleOpenHashMap[] loadToDoubleLongKeyMaps(String modelDir,
      Configuration conf)
      throws IOException {
    // Load model meta
    ModelFilesMeta meta = getMeta(modelDir, conf);
    RowType rowType = RowType.valueOf(meta.getRowType());

    // Check row type
    if (rowType != RowType.T_DOUBLE_SPARSE_LONGKEY && rowType != RowType.T_DOUBLE_SPARSE_LONGKEY_COMPONENT) {
      throw new IOException("model row type is not sparse long double, you should check it");
    }

    // Load model
    SparseDoubleLongKeyModel model = new SparseDoubleLongKeyModel(meta.getRow(), meta.getCol());
    loadModel(modelDir, model, meta, conf);

    return model.getModel();
  }

  private static void loadModel(String modelDir, Model model, ModelFilesMeta meta,
      Configuration conf)
      throws IOException {
    // Load model
    LOG.info("start to load model " + meta.getMatrixName() + " from " + modelDir);
    List<Integer> partIds = new ArrayList<>(meta.getPartMetas().keySet());
    Vector<String> errorLogs = new Vector<>();
    Path modelPath = new Path(modelDir);
    FileSystem fs = modelPath.getFileSystem(conf);

    LoadOp loadOp = new LoadOp(model, fs, modelPath, partIds, meta, errorLogs, 0, partIds.size());
    pool.execute(loadOp);
    loadOp.join();

    if (!errorLogs.isEmpty()) {
      throw new IOException(join("\n", errorLogs));
    }
  }

  private static String join(CharSequence separator, Iterable<String> strings) {
    StringBuilder sb = new StringBuilder();
    boolean first = true;
    for (String s : strings) {
      if (first) {
        first = false;
      } else {
        sb.append(separator);
      }
      sb.append(s);
    }
    return sb.toString();
  }

  public static void main(String[] args) throws IOException {
    final Configuration conf = new Configuration();
    // load hadoop configuration
    String hadoopHomePath = System.getenv("HADOOP_HOME");
    if (hadoopHomePath == null) {
      LOG.warn("HADOOP_HOME is empty.");
    } else {
      conf.addResource(new Path(hadoopHomePath + "/etc/hadoop/yarn-site.xml"));
      conf.addResource(new Path(hadoopHomePath + "/etc/hadoop/hdfs-site.xml"));
    }

    String baseDir = "out";
    String denseDoubleModelPath = baseDir + "/dense_double";
    String sparseDoubleModelPath = baseDir + "/sparse_double";
    String denseFloatModelPath = baseDir + "/dense_float";
    String sparseFloatModelPath = baseDir + "/sparse_float";
    String denseIntModelPath = baseDir + "/dense_int";
    String sparseIntModelPath = baseDir + "/sparse_int";
    String sparseDoubleLongKeyModelPath = baseDir + "/sparse_double_longkey";

    double[][] denseDoubleModel = loadToDoubleArrays(denseDoubleModelPath, conf);
    int size = denseDoubleModel.length;
    for (int i = 0; i < size; i++) {
      LOG.info("model dense_double row " + i + " sum is " + sum(denseDoubleModel[i]));
    }
    denseDoubleModel = null;

    Int2DoubleOpenHashMap[] sparseDoubleModel = loadToDoubleMaps(sparseDoubleModelPath, conf);
    size = sparseDoubleModel.length;
    for (int i = 0; i < size; i++) {
      LOG.info("model sparse_double row " + i + " sum is " + sum(sparseDoubleModel[i]));
    }
    sparseDoubleModel = null;

    float[][] denseFloatModel = loadToFloatArrays(denseFloatModelPath, conf);
    size = denseFloatModel.length;
    for (int i = 0; i < size; i++) {
      LOG.info("model dense_float row " + i + " sum is " + sum(denseFloatModel[i]));
    }
    denseFloatModel = null;

    Int2FloatOpenHashMap[] sparseFloatModel = loadToFloatMaps(sparseFloatModelPath, conf);
    size = sparseFloatModel.length;
    for (int i = 0; i < size; i++) {
      LOG.info("model sparse_float row " + i + " sum is " + sum(sparseFloatModel[i]));
    }
    sparseFloatModel = null;

    int[][] denseIntModel = loadToIntArrays(denseIntModelPath, conf);
    size = denseIntModel.length;
    for (int i = 0; i < size; i++) {
      LOG.info("model dense_int row " + i + " sum is " + sum(denseIntModel[i]));
    }
    denseIntModel = null;

    Int2IntOpenHashMap[] sparseIntModel = loadToIntMaps(sparseIntModelPath, conf);
    size = sparseIntModel.length;
    for (int i = 0; i < size; i++) {
      LOG.info("model sparse_int row " + i + " sum is " + sum(sparseIntModel[i]));
    }
    sparseIntModel = null;

    Long2DoubleOpenHashMap[] sparseDoubleLongKeyModel = loadToDoubleLongKeyMaps(
        sparseDoubleLongKeyModelPath, conf);
    size = sparseDoubleLongKeyModel.length;
    for (int i = 0; i < size; i++) {
      LOG.info(
          "model sparse_double_longkey row " + i + " sum is " + sum(sparseDoubleLongKeyModel[i]));
    }
    sparseDoubleLongKeyModel = null;
  }

  private static double sum(double[] values) {
    double sum = 0.0;
    int size = values.length;
    for (int i = 0; i < size; i++) {
      sum += values[i];
    }
    return sum;
  }

  private static double sum(Int2DoubleOpenHashMap values) {
    double sum = 0.0;
    for (double v : values.values()) {
      sum += v;
    }
    return sum;
  }

  private static double sum(float[] values) {
    double sum = 0.0;
    int size = values.length;
    for (int i = 0; i < size; i++) {
      sum += values[i];
    }
    return sum;
  }

  private static double sum(Int2FloatOpenHashMap values) {
    double sum = 0.0;
    for (float v : values.values()) {
      sum += v;
    }
    return sum;
  }

  private static long sum(int[] values) {
    long sum = 0;
    int size = values.length;
    for (int i = 0; i < size; i++) {
      sum += values[i];
    }
    return sum;
  }

  private static long sum(Int2IntOpenHashMap values) {
    long sum = 0;
    for (int v : values.values()) {
      sum += v;
    }
    return sum;
  }

  private static double sum(Long2DoubleOpenHashMap values) {
    double sum = 0;
    for (double v : values.values()) {
      sum += v;
    }
    return sum;
  }
}
