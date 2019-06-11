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

package com.tencent.angel.ps.storage.vector;

import com.tencent.angel.ml.math2.vector.DoubleVector;
import com.tencent.angel.ml.math2.vector.FloatVector;
import com.tencent.angel.ml.math2.vector.IntDoubleVector;
import com.tencent.angel.ml.math2.vector.IntFloatVector;
import com.tencent.angel.ml.math2.vector.IntIntVector;
import com.tencent.angel.ml.math2.vector.IntLongVector;
import com.tencent.angel.ml.math2.vector.IntVector;
import com.tencent.angel.ml.math2.vector.LongVector;
import com.tencent.angel.ml.math2.vector.Vector;
import com.tencent.angel.ps.storage.vector.storage.IntDoubleVectorStorage;
import com.tencent.angel.ps.storage.vector.storage.IntFloatVectorStorage;
import com.tencent.angel.ps.storage.vector.storage.IntIntVectorStorage;
import com.tencent.angel.ps.storage.vector.storage.IntLongVectorStorage;
import com.tencent.angel.ps.storage.vector.storage.LongDoubleVectorStorage;
import com.tencent.angel.ps.storage.vector.storage.LongFloatVectorStorage;
import com.tencent.angel.ps.storage.vector.storage.LongIntVectorStorage;
import com.tencent.angel.ps.storage.vector.storage.LongLongVectorStorage;

/**
 * Server row utilization
 */
public class ServerRowUtils {
  /////////////////////////////////////////////////////////////////////////////////////////////////
  // Notice: Basic type ServerRow only support Vector as inner storage now, so you can use methods
  // to get inner vector for basic type ServerRow.
  /////////////////////////////////////////////////////////////////////////////////////////////////

  /**
   * Get inner vector from server row
   *
   * @param row server row
   * @return inner vector
   */
  public static Vector getVector(ServerRow row) {
    if (row instanceof ServerIntFloatRow) {
      return getVector((ServerIntFloatRow) row);
    } else if (row instanceof ServerIntDoubleRow) {
      return getVector((ServerIntDoubleRow) row);
    } else if (row instanceof ServerIntIntRow) {
      return getVector((ServerIntIntRow) row);
    } else if (row instanceof ServerIntLongRow) {
      return getVector((ServerIntLongRow) row);
    } else if (row instanceof ServerLongFloatRow) {
      return getVector((ServerLongFloatRow) row);
    } else if (row instanceof ServerLongDoubleRow) {
      return getVector((ServerLongDoubleRow) row);
    } else if (row instanceof ServerLongIntRow) {
      return getVector((ServerLongIntRow) row);
    } else if (row instanceof ServerLongLongRow) {
      return getVector((ServerLongLongRow) row);
    } else {
      throw new UnsupportedOperationException("Only basic value type row use vector as storage");
    }
  }

  /**
   * Set inner vector to server row
   *
   * @param row server row
   * @param vector vector
   */
  public static void setVector(ServerRow row, Vector vector) {
    if (row instanceof ServerIntFloatRow) {
      setVector((ServerIntFloatRow) row, (IntFloatVector) vector);
    } else if (row instanceof ServerIntDoubleRow) {
      setVector((ServerIntDoubleRow) row, (IntDoubleVector) vector);
    } else if (row instanceof ServerIntIntRow) {
      setVector((ServerIntIntRow) row, (IntIntVector) vector);
    } else if (row instanceof ServerIntLongRow) {
      setVector((ServerIntLongRow) row, (IntLongVector) vector);
    } else if (row instanceof ServerLongFloatRow) {
      setVector((ServerLongFloatRow) row, (FloatVector) vector);
    } else if (row instanceof ServerLongDoubleRow) {
      setVector((ServerLongDoubleRow) row, (DoubleVector) vector);
    } else if (row instanceof ServerLongIntRow) {
      setVector((ServerLongIntRow) row, (IntVector) vector);
    } else if (row instanceof ServerLongLongRow) {
      setVector((ServerLongLongRow) row, (LongVector) vector);
    } else {
      throw new UnsupportedOperationException("Only basic value type row use vector as storage");
    }
  }

  public static IntDoubleVector getVector(ServerIntDoubleRow row) {
    return ((IntDoubleVectorStorage) (row.getStorage())).getVector();
  }

  public static IntFloatVector getVector(ServerIntFloatRow row) {
    return ((IntFloatVectorStorage) (row.getStorage())).getVector();
  }

  public static IntIntVector getVector(ServerIntIntRow row) {
    return ((IntIntVectorStorage) (row.getStorage())).getVector();
  }

  public static IntLongVector getVector(ServerIntLongRow row) {
    return ((IntLongVectorStorage) (row.getStorage())).getVector();
  }

  public static DoubleVector getVector(ServerLongDoubleRow row) {
    return ((LongDoubleVectorStorage) (row.getStorage())).getVector();
  }

  public static FloatVector getVector(ServerLongFloatRow row) {
    return ((LongFloatVectorStorage) (row.getStorage())).getVector();
  }

  public static IntVector getVector(ServerLongIntRow row) {
    return ((LongIntVectorStorage) (row.getStorage())).getVector();
  }

  public static LongVector getVector(ServerLongLongRow row) {
    return ((LongLongVectorStorage) (row.getStorage())).getVector();
  }


  public static void setVector(ServerIntDoubleRow row, IntDoubleVector vector) {
    ((IntDoubleVectorStorage) (row.getStorage())).setVector(vector);
  }


  public static void setVector(ServerIntFloatRow row, IntFloatVector vector) {
    ((IntFloatVectorStorage) (row.getStorage())).setVector(vector);
  }

  public static void setVector(ServerIntIntRow row, IntIntVector vector) {
    ((IntIntVectorStorage) (row.getStorage())).setVector(vector);
  }

  public static void setVector(ServerIntLongRow row, IntLongVector vector) {
    ((IntLongVectorStorage) (row.getStorage())).setVector(vector);
  }

  public static void setVector(ServerLongDoubleRow row, DoubleVector vector) {
    ((LongDoubleVectorStorage) (row.getStorage())).setVector(vector);
  }

  public static void setVector(ServerLongFloatRow row, FloatVector vector) {
    ((LongFloatVectorStorage) (row.getStorage())).setVector(vector);
  }

  public static void setVector(ServerLongIntRow row, IntVector vector) {
    ((LongIntVectorStorage) (row.getStorage())).setVector(vector);
  }

  public static void setVector(ServerLongLongRow row, LongVector vector) {
    ((LongLongVectorStorage) (row.getStorage())).setVector(vector);
  }
}
