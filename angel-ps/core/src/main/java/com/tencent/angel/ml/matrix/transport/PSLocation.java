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
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */

package com.tencent.angel.ml.matrix.transport;

import com.tencent.angel.common.location.Location;
import com.tencent.angel.ps.ParameterServerId;

/**
 * PS location
 */
public class PSLocation {
  public final ParameterServerId psId;
  public final Location loc;

  public PSLocation(ParameterServerId psId, Location loc) {
    this.psId = psId;
    this.loc = loc;
  }

  @Override public boolean equals(Object o) {
    if (this == o)
      return true;
    if (o == null || getClass() != o.getClass())
      return false;

    PSLocation that = (PSLocation) o;

    if (psId != null ? !psId.equals(that.psId) : that.psId != null)
      return false;
    return loc != null ? loc.equals(that.loc) : that.loc == null;
  }

  @Override public int hashCode() {
    int result = psId != null ? psId.hashCode() : 0;
    result = 31 * result + (loc != null ? loc.hashCode() : 0);
    return result;
  }

  @Override public String toString() {
    return "PSLocation{" + "psId=" + psId + ", loc=" + loc + '}';
  }
}
