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

package com.tencent.angel.ml.matrix.psf;

import com.tencent.angel.exception.InvalidParameterException;
import com.tencent.angel.ml.math.vector.DenseDoubleVector;
import com.tencent.angel.ml.matrix.psf.aggr.Pull;
import com.tencent.angel.ml.matrix.psf.get.single.GetRowResult;
import com.tencent.angel.ml.matrix.psf.update.*;
import com.tencent.angel.ml.matrix.psf.update.enhance.CompressUpdateFunc;
import com.tencent.angel.ml.matrix.psf.update.enhance.UpdateFunc;
import com.tencent.angel.ml.matrix.psf.update.Increment;
import com.tencent.angel.ml.matrix.psf.update.Push;
import com.tencent.angel.psagent.matrix.MatrixClient;
import com.tencent.angel.psagent.matrix.MatrixClientFactory;
import org.apache.commons.logging.LogFactory;
import org.apache.log4j.PropertyConfigurator;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.Arrays;
import java.util.concurrent.ExecutionException;

public class UpdateFuncTest {

  private static final org.apache.commons.logging.Log LOG = LogFactory.getLog(UpdateFuncTest.class);

  private static MatrixClient w2Client = null;
  private static double[] localArray0 = null;
  private static double[] localArray1 = null;
  private static double delta = 1e-6;
  private static int dim = -1;

  static {
    PropertyConfigurator.configure("../conf/log4j.properties");
  }

  @BeforeClass
  public static void setup() throws Exception {
    LocalClusterHelper.setup();
    w2Client = MatrixClientFactory.get("w2", 0);
    // row 0 is a random uniform
    w2Client.update(new RandomUniform(w2Client.getMatrixId(), 0, 0.0, 1.0)).get();
    // row 1 is a random normal
    w2Client.update(new RandomNormal(w2Client.getMatrixId(), 1, 0.0, 1.0)).get();
    // row 2 is filled with 1.0
    w2Client.update(new Fill(w2Client.getMatrixId(), 2, 1.0)).get();

    localArray0 = pull(w2Client, 0);
    localArray1 = pull(w2Client, 1);
    dim  = localArray1.length;
  }

  /**
  @Test
  public void testAbs() throws InvalidParameterException, InterruptedException, ExecutionException {
    UpdateFunc func = new Abs(w2Client.getMatrixId(), 1, 3);
    w2Client.update(func).get();

    double[] result = pull(w2Client, 3);
    assert(result.length == dim);
    for (int i = 0; i < result.length; i++) {
      Assert.assertEquals(result[i], Math.abs(localArray1[i]), delta);
    }
  }

  @Test
  public void testAdd() throws InvalidParameterException, InterruptedException, ExecutionException {
    UpdateFunc func = new Add(w2Client.getMatrixId(), 0, 1, 3);
    w2Client.update(func).get();

    double[] addResult = pull(w2Client, 3);
    assert(addResult.length == dim);
    for (int i = 0; i < addResult.length; i++) {
      Assert.assertEquals(addResult[i], localArray0[i] + localArray1[i], delta);
    }
  }

  @Test
  public void testAddS() throws InvalidParameterException, InterruptedException, ExecutionException {
    UpdateFunc func = new AddS(w2Client.getMatrixId(), 0, 3, 2.0);
    w2Client.update(func).get();

    double[] addResult = pull(w2Client, 3);
    assert(addResult.length == dim);
    for (int i = 0; i < addResult.length; i++) {
      Assert.assertEquals(addResult[i], localArray0[i] + 2.0, delta);
    }
  }

  @Test
  public void testAxpy() throws Exception {
    w2Client.update(new Fill(w2Client.getMatrixId(), 3, 1.0)).get();
    UpdateFunc func = new Axpy(w2Client.getMatrixId(), 0, 3, -2.0);
    w2Client.update(func).get();

    double[] result = pull(w2Client, 3);
    assert(result.length == dim);
    for (int i = 0; i < result.length; i++) {
      Assert.assertEquals(result[i], localArray0[i] * -2.0 + 1, delta);
    }
  }

  @Test
  public void testCeil() throws Exception {
    UpdateFunc func = new Ceil(w2Client.getMatrixId(), 0, 3);
    w2Client.update(func).get();

    double[] result = pull(w2Client, 3);
    assert(result.length == dim);
    for (int i = 0; i < result.length; i++) {
      Assert.assertEquals(result[i], Math.ceil(localArray0[i]), delta);
    }
  }

  @Test
  public void testCopy() throws Exception {
    UpdateFunc func = new Copy(w2Client.getMatrixId(), 0, 3);
    w2Client.update(func).get();

    double[] result = pull(w2Client, 3);
    assert(result.length == dim);
    for (int i = 0; i < result.length; i++) {
      Assert.assertEquals(result[i], localArray0[i], delta);
    }
  }

  @Test
  public void testDiv() throws Exception {
    UpdateFunc func = new Div(w2Client.getMatrixId(), 1, 0, 3);
    w2Client.update(func).get();

    double[] result = pull(w2Client, 3);
    assert(result.length == dim);
    for (int i = 0; i < result.length; i++) {
      Assert.assertEquals(result[i], localArray1[i] / localArray0[i], delta);
    }
  }

  @Test
  public void testDivS() throws Exception {
    UpdateFunc func = new DivS(w2Client.getMatrixId(), 0, 3, -1.0);
    w2Client.update(func).get();

    double[] result = pull(w2Client, 3);
    assert(result.length == dim);
    for (int i = 0; i < result.length; i++) {
      Assert.assertEquals(result[i], localArray0[i] / -1.0, delta);
    }
  }

  @Test
  public void testExp() throws Exception {
    UpdateFunc func = new Exp(w2Client.getMatrixId(), 0, 3);
    w2Client.update(func).get();

    double[] result = pull(w2Client, 3);
    assert(result.length == dim);
    for (int i = 0; i < result.length; i++) {
      Assert.assertEquals(result[i], Math.exp(localArray0[i]), delta);
    }
  }

  @Test
  public void testExpm1() throws Exception {
    UpdateFunc func = new Expm1(w2Client.getMatrixId(), 0, 3);
    w2Client.update(func).get();

    double[] result = pull(w2Client, 3);
    assert(result.length == dim);
    for (int i = 0; i < result.length; i++) {
      Assert.assertEquals(result[i], Math.expm1(localArray0[i]), delta);
    }
  }

  @Test
  public void testFill() throws Exception {
    UpdateFunc func = new Fill(w2Client.getMatrixId(), 3, -1.0);
    w2Client.update(func).get();

    double[] result = pull(w2Client, 3);
    assert(result.length == dim);
    for (int i = 0; i < result.length; i++) {
      Assert.assertEquals(result[i], -1.0, delta);
    }
  }

  @Test
  public void testFloor() throws Exception {
    UpdateFunc func = new Floor(w2Client.getMatrixId(), 0, 3);
    w2Client.update(func).get();

    double[] result = pull(w2Client, 3);
    assert(result.length == dim);
    for (int i = 0; i < result.length; i++) {
      Assert.assertEquals(result[i], Math.floor(localArray0[i]), delta);
    }
  }

  @Test
  public void testIncrement() throws Exception {
    UpdateFunc func = new Increment(w2Client.getMatrixId(), 3, localArray1);
    w2Client.update(func).get();

    double[] result = pull(w2Client, 3);
    assert(result.length == dim);
    for (int i = 0; i < result.length; i++) {
      Assert.assertEquals(result[i], 0.0 + localArray1[i], delta);
    }
  }

  @Test
  public void testCompress() throws Exception {

    UpdateFunc func = new CompressUpdateFunc(w2Client.getMatrixId(), 5, localArray1, 8);
    w2Client.update(func).get();

    int maxPoint = (int) Math.pow(2, 8 - 1) - 1;
    double maxMaxAbs = 0.0;
    for (int i = 0; i < localArray1.length; i++) {
      maxMaxAbs = Math.abs(localArray1[i]) > maxMaxAbs ? Math.abs(localArray1[i]): maxMaxAbs;
    }

    double[] result = pull(w2Client, 5);
    assert(result.length == dim);
    for (int i = 0; i < result.length; i++) {
      Assert.assertEquals(localArray1[i], 0.0 + result[i], 2 * maxMaxAbs / maxPoint);
    }
  }

  @Test
  public void testLog() throws Exception {
    UpdateFunc func = new Log(w2Client.getMatrixId(), 0, 3);
    w2Client.update(func).get();

    double[] result = pull(w2Client, 3);
    assert(result.length == dim);
    for (int i = 0; i < result.length; i++) {
      Assert.assertEquals(result[i], Math.log(localArray0[i]), delta);
    }
  }

  @Test
  public void testLog1p() throws Exception {
    UpdateFunc func = new Log1p(w2Client.getMatrixId(), 0, 3);
    w2Client.update(func).get();

    double[] result = pull(w2Client, 3);
    assert(result.length == dim);
    for (int i = 0; i < result.length; i++) {
      Assert.assertEquals(result[i], Math.log1p(localArray0[i]), delta);
    }
  }

  @Test
  public void testLog10() throws Exception {
    UpdateFunc func = new Log10(w2Client.getMatrixId(), 0, 3);
    w2Client.update(func).get();

    double[] result = pull(w2Client, 3);
    assert(result.length == dim);
    for (int i = 0; i < result.length; i++) {
      Assert.assertEquals(result[i], Math.log10(localArray0[i]), delta);
    }
  }

  @Test
  public void testMaxA() throws Exception {
    w2Client.update(new Fill(w2Client.getMatrixId(), 3, 0.0)).get();
    UpdateFunc func = new MaxA(w2Client.getMatrixId(), 3, localArray1);
    w2Client.update(func).get();

    double[] result = pull(w2Client, 3);
    assert(result.length == dim);
    for (int i = 0; i < result.length; i++) {
      Assert.assertEquals(result[i], Math.max(localArray1[i], 0.0), delta);
    }
  }

  @Test
  public void testMaxV() throws Exception {
    UpdateFunc func = new MaxV(w2Client.getMatrixId(), 0, 1, 3);
    w2Client.update(func).get();

    double[] result = pull(w2Client, 3);
    assert(result.length == dim);
    for (int i = 0; i < result.length; i++) {
      Assert.assertEquals(result[i], Math.max(localArray1[i], localArray0[i]), delta);
    }
  }

  @Test
  public void testMinA() throws Exception {
    w2Client.update(new Fill(w2Client.getMatrixId(), 3, 0.0)).get();
    UpdateFunc func = new MinA(w2Client.getMatrixId(), 3, localArray1);
    w2Client.update(func).get();

    double[] result = pull(w2Client, 3);
    assert(result.length == dim);
    for (int i = 0; i < result.length; i++) {
      Assert.assertEquals(result[i], Math.min(localArray1[i], 0.0), delta);
    }
  }

  @Test
  public void testMinV() throws Exception {
    UpdateFunc func = new MinV(w2Client.getMatrixId(), 0, 1, 3);
    w2Client.update(func).get();

    double[] result = pull(w2Client, 3);
    assert(result.length == dim);
    for (int i = 0; i < result.length; i++) {
      Assert.assertEquals(result[i], Math.min(localArray1[i], localArray0[i]), delta);
    }
  }

  @Test
  public void testMul() throws Exception {
    UpdateFunc func = new Mul(w2Client.getMatrixId(), 0, 1, 3);
    w2Client.update(func).get();

    double[] addResult = pull(w2Client, 3);
    assert(addResult.length == dim);
    for (int i = 0; i < addResult.length; i++) {
      Assert.assertEquals(addResult[i], localArray0[i] * localArray1[i], delta);
    }
  }

  @Test
  public void testMulS() throws Exception {
    UpdateFunc func = new MulS(w2Client.getMatrixId(), 0, 3, -1.0);
    w2Client.update(func).get();

    double[] addResult = pull(w2Client, 3);
    assert(addResult.length == dim);
    for (int i = 0; i < addResult.length; i++) {
      Assert.assertEquals(addResult[i], localArray0[i] * -1.0, delta);
    }
  }

  @Test
  public void testPow() throws Exception {
    UpdateFunc func = new Pow(w2Client.getMatrixId(), 0, 3, 3.0);
    w2Client.update(func).get();

    double[] result = pull(w2Client, 3);
    assert(result.length == dim);
    for (int i = 0; i < result.length; i++) {
      Assert.assertEquals(result[i], Math.pow(localArray0[i], 3.0), delta);
    }
  }

  @Test
  public void testPut() throws Exception {
    UpdateFunc func = new Push(w2Client.getMatrixId(), 3, localArray0);
    w2Client.update(func).get();

    double[] result = pull(w2Client, 3);
    assert(result.length == dim);
    for (int i = 0; i < result.length; i++) {
      Assert.assertEquals(result[i], localArray0[i], delta);
    }
  }

  @Test
  public void testRound() throws Exception {
    UpdateFunc func = new Round(w2Client.getMatrixId(), 0, 3);
    w2Client.update(func).get();

    double[] result = pull(w2Client, 3);
    assert(result.length == dim);
    for (int i = 0; i < result.length; i++) {
      Assert.assertEquals(result[i], Math.round(localArray0[i]), delta);
    }
  }

  @Test
  public void testScale() throws Exception {
    w2Client.update(new Push(w2Client.getMatrixId(), 3, localArray0)).get();
    UpdateFunc func = new Scale(w2Client.getMatrixId(), 3, 2.0);
    w2Client.update(func).get();

    double[] result = pull(w2Client, 3);
    assert(result.length == dim);
    for (int i = 0; i < result.length; i++) {
      Assert.assertEquals(result[i], localArray0[i] * 2.0, delta);
    }
  }

  @Test
  public void testSignum() throws Exception {
    UpdateFunc func = new Signum(w2Client.getMatrixId(), 1, 3);
    w2Client.update(func).get();

    double[] result = pull(w2Client, 3);
    assert(result.length == dim);
    for (int i = 0; i < result.length; i++) {
      Assert.assertEquals(result[i], Math.signum(localArray1[i]), delta);
    }
  }

  @Test
  public void testSqrt() throws Exception {
    UpdateFunc func = new Sqrt(w2Client.getMatrixId(), 0, 3);
    w2Client.update(func).get();

    double[] result = pull(w2Client, 3);
    assert(result.length == dim);
    for (int i = 0; i < result.length; i++) {
      Assert.assertEquals(result[i], Math.sqrt(localArray0[i]), delta);
    }
  }

  @Test
  public void testSub() throws Exception {
    UpdateFunc func = new Sub(w2Client.getMatrixId(), 0, 1, 3);
    w2Client.update(func).get();

    double[] result = pull(w2Client, 3);
    assert(result.length == dim);
    for (int i = 0; i < result.length; i++) {
      Assert.assertEquals(result[i], localArray0[i] - localArray1[i], delta);
    }
  }

  @Test
  public void testSubS() throws Exception {
    UpdateFunc func = new SubS(w2Client.getMatrixId(), 0, 3, -1.1);
    w2Client.update(func).get();

    double[] result = pull(w2Client, 3);
    assert(result.length == dim);
    for (int i = 0; i < result.length; i++) {
      Assert.assertEquals(result[i], localArray0[i] - (-1.1), delta);
    }
  }

  public void testBeyondPart() throws Exception {
    // in different part
    UpdateFunc func = new AddS(w2Client.getMatrixId(), 0, 9, 2.0);
    try {
      w2Client.update(func).get();
    } catch (Exception e) {
      System.out.println("test exception" + e.getMessage());
    }
  }
   */
  private static void printMatrix(MatrixClient client, int rowId) {
    double[] arr = pull(client, rowId);
    System.out.println(Arrays.toString(arr));
  }


  private static double[] pull(MatrixClient client, int rowId) {
    GetRowResult rowResult = (GetRowResult) client.get(new Pull(client.getMatrixId(), rowId));
    return ((DenseDoubleVector)rowResult.getRow()).getValues();
  }

  @AfterClass
  public static void stop() throws Exception{
    LocalClusterHelper.cleanup();
  }
}
