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


package com.tencent.angel.worker.task;

import com.tencent.angel.exception.AngelException;
import com.tencent.angel.ml.math2.utils.LabeledData;
import com.tencent.angel.ml.math2.storage.IntKeyVectorStorage;
import com.tencent.angel.ml.math2.vector.IntDummyVector;
import com.tencent.angel.ml.math2.vector.Vector;
import com.tencent.angel.ml.matrix.MatrixMeta;
import com.tencent.angel.worker.storage.Reader;
import it.unimi.dsi.fastutil.ints.IntOpenHashSet;

import java.io.IOException;

/**
 * The type Labeled update index base task.
 * <p>
 * Support update index
 * </p>
 */
public abstract class LabeledUpdateIndexBaseTask<KEYIN, VALUEIN>
  extends BaseTask<KEYIN, VALUEIN, LabeledData> {
  private final boolean updateIndexEnable;
  private volatile IntOpenHashSet indexSet;
  private final MatrixMeta matrixMeta;

  public LabeledUpdateIndexBaseTask(TaskContext taskContext, MatrixMeta matrixMeta)
    throws IOException {
    super(taskContext);
    this.matrixMeta = matrixMeta;
    updateIndexEnable = true;
    indexSet = new IntOpenHashSet();
  }

  @Override public void preProcess(TaskContext taskContext) {
    try {
      Reader<KEYIN, VALUEIN> reader = taskContext.getReader();
      while (reader.nextKeyValue()) {
        LabeledData out = parse(reader.getCurrentKey(), reader.getCurrentValue());
        if (out != null) {
          taskDataBlock.put(out);
          if (updateIndexEnable) {
            Vector vector = out.getX();
            int[] indexes;
            if (vector instanceof IntDummyVector) {
              indexes = ((IntDummyVector) vector).getIndices();
            } else if (vector.getStorage() instanceof IntKeyVectorStorage) {
              indexes = ((IntKeyVectorStorage) vector).getIndices();
            } else {
              throw new AngelException("");
            }

            for (int i = 0; i < indexes.length; i++) {
              indexSet.add(indexes[i]);
            }
          }
        }
      }

      taskDataBlock.flush();
    } catch (Exception e) {
      throw new AngelException("Pre-Process Error.", e);
    }
  }

  /**
   * Is update index enable boolean.
   *
   * @return true if supported else false
   */
  public boolean isUpdateIndexEnable() {
    return updateIndexEnable;
  }

  /**
   * Gets index set.
   *
   * @return the index set
   */
  public IntOpenHashSet getIndexSet() {
    return indexSet;
  }

  /**
   * Sets index set.
   *
   * @param indexSet the index set
   */
  public void setIndexSet(IntOpenHashSet indexSet) {
    this.indexSet = indexSet;
  }

  /**
   * Gets matrix meta.
   *
   * @return the matrix meta
   */
  public MatrixMeta getMatrixMeta() {
    return matrixMeta;
  }
}
