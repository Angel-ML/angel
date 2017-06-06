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
import com.tencent.angel.ml.matrix.psf.updater.*;
import com.tencent.angel.ml.matrix.psf.updater.base.UpdaterFunc;
import com.tencent.angel.psagent.matrix.MatrixClient;
import com.tencent.angel.psagent.matrix.MatrixClientFactory;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.Arrays;
import java.util.concurrent.ExecutionException;

public class UpdaterFuncTest extends SharedAngelTest {
  private static MatrixClient w2Client = null;
  private static double[] localArray0 = null;
  private static double[] localArray1 = null;
  private static double delta = 1e-6;
  private static int dim = -1;

  @BeforeClass
  public static void setup() throws Exception {
    SharedAngelTest.setup();
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

  @Test
  public void testAbs() throws InvalidParameterException, InterruptedException, ExecutionException {
    UpdaterFunc func = new Abs(w2Client.getMatrixId(), 1, 3);
    w2Client.update(func).get();

    double[] result = pull(w2Client, 3);
    assert(result.length == dim);
    for (int i = 0; i < result.length; i++) {
      Assert.assertEquals(result[i], Math.abs(localArray1[i]), delta);
    }
  }

  @Test
  public void testAdd() throws InvalidParameterException, InterruptedException, ExecutionException {
    UpdaterFunc func = new Add(w2Client.getMatrixId(), 0, 1, 3);
    w2Client.update(func).get();

    double[] addResult = pull(w2Client, 3);
    assert(addResult.length == dim);
    for (int i = 0; i < addResult.length; i++) {
      Assert.assertEquals(addResult[i], localArray0[i] + localArray1[i], delta);
    }
  }

  @Test
  public void testAddS() throws InvalidParameterException, InterruptedException, ExecutionException {
    UpdaterFunc func = new AddS(w2Client.getMatrixId(), 0, 3, 2.0);
    w2Client.update(func).get();

    double[] addResult = pull(w2Client, 3);
    assert(addResult.length == dim);
    for (int i = 0; i < addResult.length; i++) {
      Assert.assertEquals(addResult[i], localArray0[i] + 2.0, delta);
    }
  }

  @Test
  public void testAxpy() throws InvalidParameterException, InterruptedException, ExecutionException {
    w2Client.update(new Fill(w2Client.getMatrixId(), 3, 1.0)).get();
    UpdaterFunc func = new Axpy(w2Client.getMatrixId(), 0, 3, -2.0);
    w2Client.update(func).get();

    double[] result = pull(w2Client, 3);
    assert(result.length == dim);
    for (int i = 0; i < result.length; i++) {
      Assert.assertEquals(result[i], localArray0[i] * -2.0 + 1, delta);
    }
  }

  @Test
  public void testCeil() throws InvalidParameterException, InterruptedException, ExecutionException {
    UpdaterFunc func = new Ceil(w2Client.getMatrixId(), 0, 3);
    w2Client.update(func).get();

    double[] result = pull(w2Client, 3);
    assert(result.length == dim);
    for (int i = 0; i < result.length; i++) {
      Assert.assertEquals(result[i], Math.ceil(localArray0[i]), delta);
    }
  }

  @Test
  public void testCopy() throws InvalidParameterException, InterruptedException, ExecutionException {
    UpdaterFunc func = new Copy(w2Client.getMatrixId(), 0, 3);
    w2Client.update(func).get();

    double[] result = pull(w2Client, 3);
    assert(result.length == dim);
    for (int i = 0; i < result.length; i++) {
      Assert.assertEquals(result[i], localArray0[i], delta);
    }
  }

  @Test
  public void testDiv() throws InvalidParameterException, InterruptedException, ExecutionException {
    UpdaterFunc func = new Div(w2Client.getMatrixId(), 1, 0, 3);
    w2Client.update(func).get();

    double[] result = pull(w2Client, 3);
    assert(result.length == dim);
    for (int i = 0; i < result.length; i++) {
      Assert.assertEquals(result[i], localArray1[i] / localArray0[i], delta);
    }
  }

  @Test
  public void testDivS() throws InvalidParameterException, InterruptedException, ExecutionException {
    UpdaterFunc func = new DivS(w2Client.getMatrixId(), 0, 3, -1.0);
    w2Client.update(func).get();

    double[] result = pull(w2Client, 3);
    assert(result.length == dim);
    for (int i = 0; i < result.length; i++) {
      Assert.assertEquals(result[i], localArray0[i] / -1.0, delta);
    }
  }

  @Test
  public void testExp() throws InvalidParameterException, InterruptedException, ExecutionException {
    UpdaterFunc func = new Exp(w2Client.getMatrixId(), 0, 3);
    w2Client.update(func).get();

    double[] result = pull(w2Client, 3);
    assert(result.length == dim);
    for (int i = 0; i < result.length; i++) {
      Assert.assertEquals(result[i], Math.exp(localArray0[i]), delta);
    }
  }

  @Test
  public void testExpm1() throws InvalidParameterException, InterruptedException, ExecutionException {
    UpdaterFunc func = new Expm1(w2Client.getMatrixId(), 0, 3);
    w2Client.update(func).get();

    double[] result = pull(w2Client, 3);
    assert(result.length == dim);
    for (int i = 0; i < result.length; i++) {
      Assert.assertEquals(result[i], Math.expm1(localArray0[i]), delta);
    }
  }

  @Test
  public void testFill() throws InvalidParameterException, InterruptedException, ExecutionException {
    UpdaterFunc func = new Fill(w2Client.getMatrixId(), 3, -1.0);
    w2Client.update(func).get();

    double[] result = pull(w2Client, 3);
    assert(result.length == dim);
    for (int i = 0; i < result.length; i++) {
      Assert.assertEquals(result[i], -1.0, delta);
    }
  }

  @Test
  public void testFloor() throws InvalidParameterException, InterruptedException, ExecutionException {
    UpdaterFunc func = new Floor(w2Client.getMatrixId(), 0, 3);
    w2Client.update(func).get();

    double[] result = pull(w2Client, 3);
    assert(result.length == dim);
    for (int i = 0; i < result.length; i++) {
      Assert.assertEquals(result[i], Math.floor(localArray0[i]), delta);
    }
  }

  @Test
  public void testIncrement() throws InvalidParameterException, InterruptedException, ExecutionException {
    UpdaterFunc func = new Increment(w2Client.getMatrixId(), 3, localArray1);
    w2Client.update(func).get();

    double[] result = pull(w2Client, 3);
    assert(result.length == dim);
    for (int i = 0; i < result.length; i++) {
      Assert.assertEquals(result[i], 0.0 + localArray1[i], delta);
    }
  }

  @Test
  public void testLog() throws InvalidParameterException, InterruptedException, ExecutionException {
    UpdaterFunc func = new Log(w2Client.getMatrixId(), 0, 3);
    w2Client.update(func).get();

    double[] result = pull(w2Client, 3);
    assert(result.length == dim);
    for (int i = 0; i < result.length; i++) {
      Assert.assertEquals(result[i], Math.log(localArray0[i]), delta);
    }
  }

  @Test
  public void testLog1p() throws InvalidParameterException, InterruptedException, ExecutionException {
    UpdaterFunc func = new Log1p(w2Client.getMatrixId(), 0, 3);
    w2Client.update(func).get();

    double[] result = pull(w2Client, 3);
    assert(result.length == dim);
    for (int i = 0; i < result.length; i++) {
      Assert.assertEquals(result[i], Math.log1p(localArray0[i]), delta);
    }
  }

  @Test
  public void testLog10() throws InvalidParameterException, InterruptedException, ExecutionException {
    UpdaterFunc func = new Log10(w2Client.getMatrixId(), 0, 3);
    w2Client.update(func).get();

    double[] result = pull(w2Client, 3);
    assert(result.length == dim);
    for (int i = 0; i < result.length; i++) {
      Assert.assertEquals(result[i], Math.log10(localArray0[i]), delta);
    }
  }

  @Test
  public void testMaxA() throws InvalidParameterException, InterruptedException, ExecutionException {
    w2Client.update(new Fill(w2Client.getMatrixId(), 3, 0.0)).get();
    UpdaterFunc func = new MaxA(w2Client.getMatrixId(), 3, localArray1);
    w2Client.update(func).get();

    double[] result = pull(w2Client, 3);
    assert(result.length == dim);
    for (int i = 0; i < result.length; i++) {
      Assert.assertEquals(result[i], Math.max(localArray1[i], 0.0), delta);
    }
  }

  @Test
  public void testMaxV() throws InvalidParameterException, InterruptedException, ExecutionException {
    UpdaterFunc func = new MaxV(w2Client.getMatrixId(), 0, 1, 3);
    w2Client.update(func).get();

    double[] result = pull(w2Client, 3);
    assert(result.length == dim);
    for (int i = 0; i < result.length; i++) {
      Assert.assertEquals(result[i], Math.max(localArray1[i], localArray0[i]), delta);
    }
  }

  @Test
  public void testMinA() throws InvalidParameterException, InterruptedException, ExecutionException {
    w2Client.update(new Fill(w2Client.getMatrixId(), 3, 0.0)).get();
    UpdaterFunc func = new MinA(w2Client.getMatrixId(), 3, localArray1);
    w2Client.update(func).get();

    double[] result = pull(w2Client, 3);
    assert(result.length == dim);
    for (int i = 0; i < result.length; i++) {
      Assert.assertEquals(result[i], Math.min(localArray1[i], 0.0), delta);
    }
  }

  @Test
  public void testMinV() throws InvalidParameterException, InterruptedException, ExecutionException {
    UpdaterFunc func = new MinV(w2Client.getMatrixId(), 0, 1, 3);
    w2Client.update(func).get();

    double[] result = pull(w2Client, 3);
    assert(result.length == dim);
    for (int i = 0; i < result.length; i++) {
      Assert.assertEquals(result[i], Math.min(localArray1[i], localArray0[i]), delta);
    }
  }

  @Test
  public void testMul() throws InvalidParameterException, InterruptedException, ExecutionException {
    UpdaterFunc func = new Mul(w2Client.getMatrixId(), 0, 1, 3);
    w2Client.update(func).get();

    double[] addResult = pull(w2Client, 3);
    assert(addResult.length == dim);
    for (int i = 0; i < addResult.length; i++) {
      Assert.assertEquals(addResult[i], localArray0[i] * localArray1[i], delta);
    }
  }

  @Test
  public void testMulS() throws InvalidParameterException, InterruptedException, ExecutionException {
    UpdaterFunc func = new MulS(w2Client.getMatrixId(), 0, 3, -1.0);
    w2Client.update(func).get();

    double[] addResult = pull(w2Client, 3);
    assert(addResult.length == dim);
    for (int i = 0; i < addResult.length; i++) {
      Assert.assertEquals(addResult[i], localArray0[i] * -1.0, delta);
    }
  }

  @Test
  public void testPow() throws InvalidParameterException, InterruptedException, ExecutionException {
    UpdaterFunc func = new Pow(w2Client.getMatrixId(), 0, 3, 3.0);
    w2Client.update(func).get();

    double[] result = pull(w2Client, 3);
    assert(result.length == dim);
    for (int i = 0; i < result.length; i++) {
      Assert.assertEquals(result[i], Math.pow(localArray0[i], 3.0), delta);
    }
  }

  @Test
  public void testPut() throws InvalidParameterException, InterruptedException, ExecutionException {
    UpdaterFunc func = new Put(w2Client.getMatrixId(), 3, localArray0);
    w2Client.update(func).get();

    double[] result = pull(w2Client, 3);
    assert(result.length == dim);
    for (int i = 0; i < result.length; i++) {
      Assert.assertEquals(result[i], localArray0[i], delta);
    }
  }

  @Test
  public void testRound() throws InvalidParameterException, InterruptedException, ExecutionException {
    UpdaterFunc func = new Round(w2Client.getMatrixId(), 0, 3);
    w2Client.update(func).get();

    double[] result = pull(w2Client, 3);
    assert(result.length == dim);
    for (int i = 0; i < result.length; i++) {
      Assert.assertEquals(result[i], Math.round(localArray0[i]), delta);
    }
  }

  @Test
  public void testScale() throws InvalidParameterException, InterruptedException, ExecutionException {
    w2Client.update(new Put(w2Client.getMatrixId(), 3, localArray0)).get();
    UpdaterFunc func = new Scale(w2Client.getMatrixId(), 3, 2.0);
    w2Client.update(func).get();

    double[] result = pull(w2Client, 3);
    assert(result.length == dim);
    for (int i = 0; i < result.length; i++) {
      Assert.assertEquals(result[i], localArray0[i] * 2.0, delta);
    }
  }

  @Test
  public void testSignum() throws InvalidParameterException, InterruptedException, ExecutionException {
    UpdaterFunc func = new Signum(w2Client.getMatrixId(), 1, 3);
    w2Client.update(func).get();

    double[] result = pull(w2Client, 3);
    assert(result.length == dim);
    for (int i = 0; i < result.length; i++) {
      Assert.assertEquals(result[i], Math.signum(localArray1[i]), delta);
    }
  }

  @Test
  public void testSqrt() throws InvalidParameterException, InterruptedException, ExecutionException {
    UpdaterFunc func = new Sqrt(w2Client.getMatrixId(), 0, 3);
    w2Client.update(func).get();

    double[] result = pull(w2Client, 3);
    assert(result.length == dim);
    for (int i = 0; i < result.length; i++) {
      Assert.assertEquals(result[i], Math.sqrt(localArray0[i]), delta);
    }
  }

  @Test
  public void testSub() throws InvalidParameterException, InterruptedException, ExecutionException {
    UpdaterFunc func = new Sub(w2Client.getMatrixId(), 0, 1, 3);
    w2Client.update(func).get();

    double[] result = pull(w2Client, 3);
    assert(result.length == dim);
    for (int i = 0; i < result.length; i++) {
      Assert.assertEquals(result[i], localArray0[i] - localArray1[i], delta);
    }
  }

  @Test
  public void testSubS() throws InvalidParameterException, InterruptedException, ExecutionException {
    UpdaterFunc func = new SubS(w2Client.getMatrixId(), 0, 3, -1.1);
    w2Client.update(func).get();

    double[] result = pull(w2Client, 3);
    assert(result.length == dim);
    for (int i = 0; i < result.length; i++) {
      Assert.assertEquals(result[i], localArray0[i] - (-1.1), delta);
    }
  }

  private static void printMatrix(MatrixClient client, int rowId) {
    double[] arr = pull(client, rowId);
    System.out.println(Arrays.toString(arr));
  }


  private static double[] pull(MatrixClient client, int rowId) {
    GetRowResult rowResult = (GetRowResult) client.get(new Pull(client.getMatrixId(), rowId));
    return ((DenseDoubleVector)rowResult.getRow()).getValues();
  }
}
