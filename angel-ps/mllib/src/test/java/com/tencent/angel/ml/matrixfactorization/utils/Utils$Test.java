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
 *
 */

package com.tencent.angel.ml.matrixfactorization.utils;

import com.tencent.angel.ml.math.vector.DenseFloatVector;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.log4j.PropertyConfigurator;
import org.junit.Test;

import static org.junit.Assert.assertEquals;


public class Utils$Test {
  private static final Log LOG = LogFactory.getLog(Utils$Test.class);
  static {
    PropertyConfigurator.configure("../conf/log4j.properties");
  }

  @Test
  public void testSgdOneItemVec() throws Exception {
    int itemId = 1;
    int userId = 2;
    int userIds[] = {2};
    int itemIds[] = {1};
    int ratings[] = {3};
    int rank = 4;
    ItemVec itemVec = new ItemVec(itemId, userIds, ratings);
    UserVec userVec = new UserVec(userId, itemIds, ratings);
    DenseFloatVector Li = new DenseFloatVector(4, new float[] {1.0f, 0.3f, 0.5f, 0.7f});
    DenseFloatVector Rj = new DenseFloatVector(4, new float[] {2f, 0.4f, 0.6f, 0.8f});
    userVec.setFeatures(Li);
    scala.collection.mutable.HashMap<Object, UserVec> users =
        new scala.collection.mutable.HashMap<>();
    users.put(2, userVec);
    DenseFloatVector update = Utils.sgdOneItemVec(itemVec, Rj, users, 0.0054, 0.01, rank);
    float trueValues[] = {0.000108f, 0.0000432f, 0.0000756f, 0.000108f};
    float values[] = update.getValues();
    for (int i = 0; i < values.length; i++)
      assertEquals(trueValues[i], values[i], 0.0000001f);

  }

  @Test
  public void testLossOneItemVec() throws Exception {
    int itemId = 1;
    int userId = 2;
    int userIds[] = {2};
    int itemIds[] = {1};
    int ratings[] = {3};
    int rank = 4;
    ItemVec itemVec = new ItemVec(itemId, userIds, ratings);
    UserVec userVec = new UserVec(userId, itemIds, ratings);
    DenseFloatVector Li = new DenseFloatVector(4, new float[] {1.0f, 0.3f, 0.5f, 0.7f});
    DenseFloatVector Rj = new DenseFloatVector(4, new float[] {2f, 0.4f, 0.6f, 0.8f});
    userVec.setFeatures(Li);
    scala.collection.mutable.HashMap<Object, UserVec> users =
        new scala.collection.mutable.HashMap<>();
    users.put(2, userVec);
    double loss = Utils.lossOneItemVec(itemVec, Rj, users, 0.0054, 0.01, rank);
    assertEquals(0.0004, loss, 0.00000001f);
  }

  @Test
  public void testSgdOneRating() throws Exception {
    DenseFloatVector Li = new DenseFloatVector(4, new float[] {1.0f, 0.3f, 0.5f, 0.7f});
    DenseFloatVector Rj = new DenseFloatVector(4, new float[] {2f, 0.4f, 0.6f, 0.8f});
    DenseFloatVector update = new DenseFloatVector(4, new float[] {0.0f, 0.0f, 0.0f, 0.0f});
    Utils.sgdOneRating(Li, Rj, 3, 0.0054, 0.01, update);
    float trueValues[] = {0.000108f, 0.0000432f, 0.0000756f, 0.000108f};
    float values[] = update.getValues();
    for (int i = 0; i < values.length; i++)
      assertEquals(trueValues[i], values[i], 0.0000001f);
  }

  @Test
  public void testLossOneRating() throws Exception {
    DenseFloatVector Li = new DenseFloatVector(4, new float[] {1.0f, 0.3f, 0.5f, 0.7f});
    DenseFloatVector Rj = new DenseFloatVector(4, new float[] {2f, 0.4f, 0.6f, 0.8f});
    double e = Utils.lossOneRating(Li, Rj, 3, 0.0054, 0.01, 4);
    assertEquals(0.0004, e, 0.00000001f);
  }


    @Test
    public void testLossOneRow() throws Exception {
        DenseFloatVector Rj = new DenseFloatVector(4, new float[] {2f, 0.4f, 0.6f, 0.8f});
        double e = Utils.lossOneRow(Rj,0.01);
        assertEquals(0.0258, e, 0.00001f);
    }
}
