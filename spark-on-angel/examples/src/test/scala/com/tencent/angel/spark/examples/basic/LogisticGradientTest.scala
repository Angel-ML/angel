package com.tencent.angel.spark.examples.basic

import com.tencent.angel.ml.feature.LabeledData
import com.tencent.angel.ml.math2.VFactory
import com.tencent.angel.ml.math2.vector.IntFloatVector
import org.junit.Assert._
import org.junit.Test

class LogisticGradientTest {
  @Test
  def sumsTheTrueLogisticGradientWithoutChangingInputs(): Unit = {
    val weight = VFactory.denseFloatVector(2)
    val first = VFactory.denseFloatVector(Array(2.0f, 0.0f))
    val second = VFactory.denseFloatVector(Array(0.0f, 4.0f))
    val data = Array(new LabeledData(first, 1), new LabeledData(second, -1))
    val gradient = LogisticRegression.calculate(weight, data).asInstanceOf[IntFloatVector]
    assertEquals(-1.0, gradient.get(0).toDouble, 1e-8)
    assertEquals(2.0, gradient.get(1).toDouble, 1e-8)
    assertEquals(0.0, weight.get(0).toDouble, 0)
    assertEquals(2.0, first.get(0).toDouble, 0)
    assertEquals(4.0, second.get(1).toDouble, 0)
    val empty = LogisticRegression.calculate(weight, Array.empty[LabeledData]).asInstanceOf[IntFloatVector]
    assertEquals(0.0, empty.sum(), 0)
  }

  @Test
  def libsvmIndicesAndBoundaryAreExplicit(): Unit = {
    val parsed = LogisticRegression.parseSample("0 1:2 123:4", 123, 1)
    val vector = parsed.getX.asInstanceOf[IntFloatVector]
    assertEquals(-1.0, parsed.getY, 0)
    assertEquals(2.0, vector.get(0).toDouble, 0)
    assertEquals(4.0, vector.get(122).toDouble, 0)
    try {
      LogisticRegression.parseSample("1 124:1", 123, 1)
      fail("Out-of-range feature must be rejected")
    } catch { case _: IllegalArgumentException => }
  }
}
