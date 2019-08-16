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
package com.tencent.angel.utils;

public class ArrayUtils {
  public static int intersectCount(long [] array1, long [] array2) {
    if (array1 == null || array2 == null || array1.length == 0 || array2.length == 0) return 0;
    int count = 0;
    int pointerA = 0;
    int pointerB = 0;
    while (pointerA < array1.length && pointerB < array2.length) {
      if (array1[pointerA] < array2[pointerB]) pointerA++;
      else if (array1[pointerA] > array2[pointerB]) pointerB++;
      else {
        count++;
        pointerA++;
        pointerB++;
      }
    }

    return count;
  }

  public static int unionCount(long [] array1, long [] array2) {
    if (array1 == null || array2 == null)
      return 0;
    else if (array1.length == 0)
      return array2.length;
    else if (array2.length == 0)
      return array1.length;

    int count = 0;
    int pointerA = 0;
    int pointerB = 0;

    while (pointerA < array1.length && pointerB < array2.length) {
      if (array1[pointerA] < array2[pointerB]) {
        pointerA++;
      } else if (array1[pointerA] > array2[pointerB]) {
        pointerB++;
      } else {
        pointerA++;
        pointerB++;
      }
      count++;
    }
    if (pointerA < array1.length)
      count += array1.length - pointerA;
    if (pointerB < array2.length)
      count += array2.length - pointerB;

    return count;
  }
}
