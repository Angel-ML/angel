/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.tencent.angel.spark.graphx.ps

import java.util

import scala.collection.mutable
import scala.language.implicitConversions
import scala.reflect.ClassTag
import scala.collection.JavaConverters._

import breeze.linalg.{DenseVector => BDV, Matrix => BM, SparseVector => BSV, Vector => BV}
import com.tencent.angel.ml.math.TVector
import com.tencent.angel.ml.math.vector.{DenseDoubleVector, TDoubleVector}
import com.tencent.angel.ml.matrix.psf.get.multi._
import com.tencent.angel.ml.matrix.psf.aggr.primitive.Pull
import com.tencent.angel.ml.matrix.psf.update.enhance.UpdateFunc
import com.tencent.angel.ml.matrix.psf.update.primitive.Push
import com.tencent.angel.protobuf.generated.MLProtos
import com.tencent.angel.psagent.matrix.{MatrixClient, MatrixClientFactory, ResponseType, Result}
import com.tencent.angel.spark.PSContext
import com.tencent.angel.spark.graphx.DataType
import org.apache.spark.SparkException
import org.apache.spark.ml.linalg.Vectors
import org.apache.spark.ml.linalg.{DenseMatrix => SDM, DenseVector => SDV, SparseVector => SSV, Vector => SV}

trait Operation[VD] extends Serializable {
  def dataType: DataType

  def remove(psName: String): Unit

  def create(dense: Boolean, rowNum: Int, columnNum: Int): String

  def update(psName: String, indices: Array[Int], values: Array[VD]): Unit

  def inc(psName: String, indices: Array[Int], values: Array[VD]): Unit

  def get(psName: String, indices: Array[Int]): Array[VD]
}

private[graphx] abstract class ValOperation[VD: ClassTag] extends Operation[VD] {

  type PSArray = Array[Double]

  protected def vdTag: ClassTag[VD] = implicitly[ClassTag[VD]]

  def javaArray2PsArray(values: Array[VD]): Array[PSArray]

  def psArray2JavaArray(values: Array[PSArray]): Array[VD]

  def javaArray2UpdateFuncArray(
    matrixId: Int,
    indices: Array[Int],
    values: Array[VD]): Array[UpdateFunc] = {
    indices.zip(javaArray2PsArray(values)).map { case (rowId, value) =>
      new Push(matrixId, rowId, value).asInstanceOf[UpdateFunc]
    }
  }

  def javaArray2TVectorArray(
    matrixId: Int,
    indices: Array[Int],
    values: Array[VD]): Array[TVector] = {
    indices.zip(javaArray2PsArray(values)).map { case (rowId, value) =>
      val vector: TVector = new DenseDoubleVector(1, value)
      vector.setMatrixId(matrixId)
      vector.setRowId(rowId)
      vector
    }
  }

  def tVectorArray2JavaArray(values: Array[TVector]): Array[VD] = {
    psArray2JavaArray(values.map { v =>
      v.asInstanceOf[TDoubleVector].getValues
    })
  }

  override def remove(psName: String): Unit = {
    val context = PSContext.getOrCreate()
    context.destroyMatrix(psName.toInt)
  }

  override def create(
    dense: Boolean = false,
    rowNum: Int = Int.MaxValue,
    colNum: Int = 1): String = {
    val context = PSContext.getOrCreate()
    val rowType = dataType match {
      case DataType.Integer =>
        if (dense) MLProtos.RowType.T_INT_DENSE else MLProtos.RowType.T_INT_SPARSE
      case DataType.Double =>
        if (dense) MLProtos.RowType.T_DOUBLE_DENSE else MLProtos.RowType.T_DOUBLE_SPARSE
      case DataType.Float =>
        if (dense) MLProtos.RowType.T_FLOAT_DENSE else MLProtos.RowType.T_FLOAT_SPARSE
    }
    context.createMatrix(rowNum, colNum, rowType).toString
  }

  private def assertSuccess(result: Result): Unit = {
    if (result.getResponseType == ResponseType.FAILED) {
      throw new SparkException("PS computation failed!")
    }
  }

  override def update(
    psName: String,
    indices: Array[Int], values: Array[VD]): Unit = {
    val context = PSContext.getOrCreate()
    val client = context.getMatrixClient(psName.toInt)
    val matrixId = psName.toInt
    javaArray2UpdateFuncArray(matrixId, indices, values).foreach { func =>
      val result = client.update(func).get
      assertSuccess(result)
    }
  }

  override def inc(
    psName: String,
    indices: Array[Int], values: Array[VD]): Unit = {
    val context = PSContext.getOrCreate()
    val matrixId = psName.toInt
    val client = context.getMatrixClient(psName.toInt)
    javaArray2TVectorArray(matrixId, indices, values).foreach { v =>
      client.increment(v)
    }
  }

  override def get(psName: String, indices: Array[Int]): Array[VD] = {
    val context = PSContext.getOrCreate()
    val matrixId = psName.toInt
    val client = context.getMatrixClient(psName.toInt)
    val list = new util.ArrayList[Integer]()
    indices.foreach(r => list.add(r))
    val func = new GetRowsFunc(new GetRowsParam(matrixId, list))
    val result = client.get(func).asInstanceOf[GetRowsResult]
    assertSuccess(result)
    val rows = result.getRows.asScala.map { case (k, v) =>
      assert(k == v.getRowId || v.getRowId == -1)
      if (v.getRowId == -1) {
        v.setRowId(k)
      }
      v
    }.toArray
    tVectorArray2JavaArray(rows)
  }
}

object Operation {

  object IntegerOperation extends ValOperation[Int] {
    override def dataType: DataType = DataType.Integer

    override def create(
      dense: Boolean,
      rowNum: Int,
      colNum: Int): String = {
      val context = PSContext.getOrCreate()
      val rowType = MLProtos.RowType.T_DOUBLE_DENSE
      context.createMatrix(rowNum, 1, rowType).toString
    }

    override def javaArray2PsArray(values: Array[Int]): Array[PSArray] = {
      values.map { v =>
        Array.fill(1) {
          v.toDouble
        }
      }
    }

    override def psArray2JavaArray(values: Array[PSArray]): Array[Int] = {
      values.map(v => v.head.toInt)
    }
  }

  object DoubleOperation extends ValOperation[Double] {

    override def dataType: DataType = DataType.Double

    override def create(
      dense: Boolean,
      rowNum: Int,
      colNum: Int): String = {
      val context = PSContext.getOrCreate()
      val rowType = MLProtos.RowType.T_DOUBLE_DENSE
      context.createMatrix(rowNum, 1, rowType).toString
    }

    override def javaArray2PsArray(values: Array[Double]): Array[PSArray] = {
      values.map { v =>
        Array.fill(1) {
          v
        }
      }
    }

    override def psArray2JavaArray(values: Array[PSArray]): Array[Double] = {
      values.map(v => v.head)
    }

  }

  object FloatOperation extends ValOperation[Float] {

    override def dataType: DataType = DataType.Float

    override def create(
      dense: Boolean,
      rowNum: Int,
      colNum: Int): String = {
      val context = PSContext.getOrCreate()
      val rowType = MLProtos.RowType.T_DOUBLE_DENSE
      context.createMatrix(rowNum, 1, rowType).toString
    }

    override def javaArray2PsArray(values: Array[Float]): Array[PSArray] = {
      values.map { v =>
        Array.fill(1) {
          v.toDouble
        }
      }
    }

    override def psArray2JavaArray(values: Array[PSArray]): Array[Float] = {
      values.map(v => v.head.toFloat)
    }

  }

  object LongOperation extends ValOperation[Long] {

    override def dataType: DataType = DataType.Long

    override def create(
      dense: Boolean,
      rowNum: Int,
      colNum: Int): String = {
      val context = PSContext.getOrCreate()
      val rowType = MLProtos.RowType.T_DOUBLE_DENSE
      context.createMatrix(rowNum, 1, rowType).toString
    }

    override def javaArray2PsArray(values: Array[Long]): Array[PSArray] = {
      values.map { v =>
        Array.fill(1) {
          v.toDouble
        }
      }
    }

    override def psArray2JavaArray(values: Array[PSArray]): Array[Long] = {
      values.map(v => v.head.toLong)
    }
  }

  object IntegerBVOperation extends ValOperation[BV[Int]] {

    override def dataType: DataType = DataType.Integer

    override def javaArray2PsArray(values: Array[BV[Int]]): Array[PSArray] = {
      values.map { v =>
        v.valuesIterator.map(_.toDouble).toArray
      }
    }

    override def psArray2JavaArray(values: Array[PSArray]): Array[BV[Int]] = {
      values.map(v => BV(v.map(_.toInt)))
    }
  }

  object DoubleBVOperation extends ValOperation[BV[Double]] {

    override def dataType: DataType = DataType.Double

    override def javaArray2PsArray(values: Array[BV[Double]]): Array[PSArray] = {
      values.map { v =>
        v.valuesIterator.toArray
      }
    }

    override def psArray2JavaArray(values: Array[PSArray]): Array[BV[Double]] = {
      values.map(v => BV(v))
    }
  }

  object FloatBVOperation extends ValOperation[BV[Float]] {

    override def dataType: DataType = DataType.Float

    override def javaArray2PsArray(values: Array[BV[Float]]): Array[PSArray] = {
      values.map { v =>
        v.valuesIterator.map(_.toDouble).toArray
      }
    }

    override def psArray2JavaArray(values: Array[PSArray]): Array[BV[Float]] = {
      values.map(v => BV(v.map(_.toFloat)))
    }
  }

  object LongBVOperation extends ValOperation[BV[Long]] {

    override def dataType: DataType = DataType.Long

    override def javaArray2PsArray(values: Array[BV[Long]]): Array[PSArray] = {
      values.map { v =>
        v.valuesIterator.map(_.toDouble).toArray
      }
    }

    override def psArray2JavaArray(values: Array[PSArray]): Array[BV[Long]] = {
      values.map(v => BV(v.map(_.toLong)))
    }
  }

  object IntegerBSVOperation extends ValOperation[BSV[Int]] {

    override def dataType: DataType = DataType.Integer

    override def javaArray2PsArray(values: Array[BSV[Int]]): Array[PSArray] = {
      values.map { v =>
        v.valuesIterator.map(_.toDouble).toArray
      }
    }

    override def psArray2JavaArray(values: Array[PSArray]): Array[BSV[Int]] = {
      values.map(v => BSV(v.map(_.toInt)))
    }
  }

  object DoubleBSVOperation extends ValOperation[BSV[Double]] {

    override def dataType: DataType = DataType.Double

    override def javaArray2PsArray(values: Array[BSV[Double]]): Array[PSArray] = {
      values.map { v =>
        v.valuesIterator.toArray
      }
    }

    override def psArray2JavaArray(values: Array[PSArray]): Array[BSV[Double]] = {
      values.map(v => BSV(v))
    }
  }

  object FloatBSVOperation extends ValOperation[BSV[Float]] {

    override def dataType: DataType = DataType.Float

    override def javaArray2PsArray(values: Array[BSV[Float]]): Array[PSArray] = {
      values.map { v =>
        v.valuesIterator.map(_.toDouble).toArray
      }
    }

    override def psArray2JavaArray(values: Array[PSArray]): Array[BSV[Float]] = {
      values.map(v => BSV(v.map(_.toFloat)))
    }
  }

  object LongBSVOperation extends ValOperation[BSV[Long]] {

    override def dataType: DataType = DataType.Long

    override def javaArray2PsArray(values: Array[BSV[Long]]): Array[PSArray] = {
      values.map { v =>
        v.valuesIterator.map(_.toDouble).toArray
      }
    }

    override def psArray2JavaArray(values: Array[PSArray]): Array[BSV[Long]] = {
      values.map(v => BSV(v.map(_.toLong)))
    }
  }

  object IntegerBDVOperation extends ValOperation[BDV[Int]] {

    override def dataType: DataType = DataType.Integer

    override def javaArray2PsArray(values: Array[BDV[Int]]): Array[PSArray] = {
      values.map { v =>
        v.valuesIterator.map(_.toDouble).toArray
      }
    }

    override def psArray2JavaArray(values: Array[PSArray]): Array[BDV[Int]] = {
      values.map(v => BDV(v.map(_.toInt)))
    }
  }

  object DoubleBDVOperation extends ValOperation[BDV[Double]] {

    override def dataType: DataType = DataType.Double

    override def javaArray2PsArray(values: Array[BDV[Double]]): Array[PSArray] = {
      values.map { v =>
        v.valuesIterator.toArray
      }
    }

    override def psArray2JavaArray(values: Array[PSArray]): Array[BDV[Double]] = {
      values.map(v => BDV(v))
    }
  }

  object FloatBDVOperation extends ValOperation[BDV[Float]] {

    override def dataType: DataType = DataType.Integer

    override def javaArray2PsArray(values: Array[BDV[Float]]): Array[PSArray] = {
      values.map { v =>
        v.valuesIterator.map(_.toDouble).toArray
      }
    }

    override def psArray2JavaArray(values: Array[PSArray]): Array[BDV[Float]] = {
      values.map(v => BDV(v.map(_.toFloat)))
    }
  }

  object LongBDVOperation extends ValOperation[BDV[Long]] {

    override def dataType: DataType = DataType.Integer

    override def javaArray2PsArray(values: Array[BDV[Long]]): Array[PSArray] = {
      values.map { v =>
        v.valuesIterator.map(_.toDouble).toArray
      }
    }

    override def psArray2JavaArray(values: Array[PSArray]): Array[BDV[Long]] = {
      values.map(v => BDV(v.map(_.toLong)))
    }
  }

  object SVOperation extends ValOperation[SV] {
    override def dataType: DataType = DataType.Double

    override def javaArray2PsArray(values: Array[SV]): Array[PSArray] = {
      values.map { v =>
        v.toArray
      }
    }

    override def psArray2JavaArray(values: Array[PSArray]): Array[SV] = {
      values.map(v => Vectors.dense(v))
    }
  }

  object SSVOperation extends ValOperation[SSV] {
    override def dataType: DataType = DataType.Double

    override def javaArray2PsArray(values: Array[SSV]): Array[PSArray] = {
      values.map { v =>
        v.toArray
      }
    }

    override def psArray2JavaArray(values: Array[PSArray]): Array[SSV] = {
      values.map { v =>
        Vectors.sparse(v.length, v.indices.toArray, v).asInstanceOf[SSV]
      }
    }
  }

  object SDVOperation extends ValOperation[SDV] {
    override def dataType: DataType = DataType.Double

    override def javaArray2PsArray(values: Array[SDV]): Array[PSArray] = {
      values.map { v =>
        v.toArray
      }
    }

    override def psArray2JavaArray(values: Array[PSArray]): Array[SDV] = {
      values.map { v =>
        Vectors.dense(v).asInstanceOf[SDV]
      }
    }
  }

  lazy val class2Operation: mutable.OpenHashMap[Class[_], Operation[_]] = {
    val hash = new mutable.OpenHashMap[Class[_], Operation[_]]()
    hash.put(classOf[Int], IntegerOperation)
    hash.put(classOf[Float], FloatOperation)
    hash.put(classOf[Double], DoubleOperation)
    hash.put(classOf[Long], LongOperation)

    hash.put(classOf[SV], SVOperation)
    hash.put(classOf[SSV], SSVOperation)
    hash.put(classOf[SDV], SDVOperation)

    hash.put(classOf[BV[Int]], IntegerBVOperation)
    hash.put(classOf[BV[Float]], FloatBVOperation)
    hash.put(classOf[BV[Double]], DoubleBVOperation)
    hash.put(classOf[BV[Long]], LongBVOperation)

    hash.put(classOf[BSV[Int]], IntegerBSVOperation)
    hash.put(classOf[BSV[Float]], FloatBSVOperation)
    hash.put(classOf[BSV[Double]], DoubleBSVOperation)
    hash.put(classOf[BSV[Long]], LongBSVOperation)

    hash.put(classOf[BDV[Int]], IntegerBDVOperation)
    hash.put(classOf[BDV[Float]], FloatBDVOperation)
    hash.put(classOf[BDV[Double]], DoubleBDVOperation)
    hash.put(classOf[BDV[Long]], LongBDVOperation)
    hash
  }

  def remove[VD](psName: String)(implicit tag: ClassTag[VD]): Unit = {
    tag2Operation(tag).remove(psName)
  }

  def create[VD](dense: Boolean, rowNum: Int, columnNum: Int)
    (implicit tag: ClassTag[VD]): String = {
    tag2Operation(tag).create(dense, rowNum, columnNum)
  }

  def update[VD](psName: String, indices: Array[Int], values: Array[VD])
    (implicit tag: ClassTag[VD]): Unit = {
    tag2Operation(tag).update(psName, indices, values)
  }

  def inc[VD](psName: String, indices: Array[Int], values: Array[VD])
    (implicit tag: ClassTag[VD]): Unit = {
    tag2Operation(tag).inc(psName, indices, values)
  }

  def get[VD](psName: String, indices: Array[Int])
    (implicit tag: ClassTag[VD]): Array[VD] = {
    tag2Operation(tag).get(psName, indices)
  }

  private def tag2Operation[VD](tag: ClassTag[VD]): Operation[VD] = {
    val vdClass = tag.runtimeClass
    if (class2Operation.contains(vdClass)) {
      class2Operation(vdClass).asInstanceOf[Operation[VD]]
    } else {
      throw new IllegalArgumentException(s"Unsupported type: $vdClass")
    }
  }
}
