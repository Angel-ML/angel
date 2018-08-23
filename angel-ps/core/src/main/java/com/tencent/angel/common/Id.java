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


package com.tencent.angel.common;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;


@InterfaceAudience.Public @InterfaceStability.Stable public abstract class Id
  implements WritableComparable<Id> {
  public static final String SEPARATOR = "_";
  protected int index;

  /**
   * constructs an ID object from the given int
   */
  public Id(int index) {
    this.index = index;
  }

  protected Id() {
  }

  /**
   * returns the int which represents the identifier
   */
  public int getIndex() {
    return index;
  }

  @Override public String toString() {
    return String.valueOf(index);
  }

  @Override public int hashCode() {
    return index;
  }

  @Override public boolean equals(Object o) {
    if (this == o)
      return true;
    if (o == null)
      return false;
    if (o.getClass() == this.getClass()) {
      Id that = (Id) o;
      return this.index == that.index;
    } else
      return false;
  }

  /**
   * Compare IDs by associated numbers
   */
  public int compareTo(Id that) {
    return this.index - that.index;
  }

  public void readFields(DataInput in) throws IOException {
    this.index = in.readInt();
  }

  public void write(DataOutput out) throws IOException {
    out.writeInt(index);
  }
}
