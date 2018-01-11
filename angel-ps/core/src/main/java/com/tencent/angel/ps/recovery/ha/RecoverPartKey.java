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

package com.tencent.angel.ps.recovery.ha;

import com.tencent.angel.PartitionKey;
import com.tencent.angel.ml.matrix.transport.PSLocation;

/**
 * Recover partition information
 */
public class RecoverPartKey {
  /**
   * Matrix id
   */
  public final PartitionKey partKey;

  /**
   * Need recover slave ps
   */
  public final PSLocation psLoc;

  /**
   * Create RecoverPartKey
   * @param partKey Matrix partition key
   * @param psLoc PS location
   */
  public RecoverPartKey(PartitionKey partKey, PSLocation psLoc) {
    this.partKey = partKey;
    this.psLoc = psLoc;
  }

  @Override public boolean equals(Object o) {
    if (this == o)
      return true;
    if (o == null || getClass() != o.getClass())
      return false;

    RecoverPartKey that = (RecoverPartKey) o;

    if (partKey != null ? !partKey.equals(that.partKey) : that.partKey != null)
      return false;
    return psLoc != null ? psLoc.equals(that.psLoc) : that.psLoc == null;
  }

  @Override public int hashCode() {
    int result = partKey != null ? partKey.hashCode() : 0;
    result = 31 * result + (psLoc != null ? psLoc.hashCode() : 0);
    return result;
  }
}
