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

package com.tencent.angel.ml.optimizer2.utils.oputils

import com.tencent.angel.ml.math.vector._

object TOperation extends Enumeration {
  type TOperation = Value
  val Add, Sub, Mul, Div, Pow = Value
}

trait Expression {
  val esp: Float = 1e-8f
  val clip: Float = 1e6f

  def clip(value: Double): Double = {
    if (value > clip) {
      clip
    } else if (value < -clip) {
      -clip
    } else {
      value
    }
  }

  def clip(value: Float): Float = {
    if (value > clip) {
      clip
    } else if (value < -clip) {
      -clip
    } else {
      value
    }
  }
}

trait Unary extends Expression {
  def apply(v1: DenseDoubleVector): DenseDoubleVector

  def apply(v1: SparseDoubleVector): SparseDoubleVector

  def apply(v1: SparseLongKeyDoubleVector): SparseLongKeyDoubleVector

  def apply(v1: DenseFloatVector): DenseFloatVector

  def apply(v1: SparseFloatVector): SparseFloatVector

  def apply(v1: SparseLongKeyFloatVector): SparseLongKeyFloatVector
}

trait Binary extends Expression {
  def apply(v1: DenseDoubleVector, v2: TDoubleVector): DenseDoubleVector

  def apply(v1: SparseDoubleVector, v2: TDoubleVector): SparseDoubleVector

  def apply(v1: SparseLongKeyDoubleVector, v2: TLongDoubleVector): SparseLongKeyDoubleVector

  def apply(v1: DenseFloatVector, v2: TFloatVector): DenseFloatVector

  def apply(v1: SparseFloatVector, v2: TFloatVector): SparseFloatVector

  def apply(v1: SparseLongKeyFloatVector, v2: TLongFloatVector): SparseLongKeyFloatVector
}

trait Ternary extends Expression {
  def apply(v1: DenseDoubleVector, v2: TDoubleVector, v3: TDoubleVector): DenseDoubleVector

  def apply(v1: SparseDoubleVector, v2: TDoubleVector, v3: TDoubleVector): SparseDoubleVector

  def apply(v1: SparseLongKeyDoubleVector, v2: TLongDoubleVector, v3: TLongDoubleVector): SparseLongKeyDoubleVector

  def apply(v1: DenseFloatVector, v2: TFloatVector, v3: TFloatVector): DenseFloatVector

  def apply(v1: SparseFloatVector, v2: TFloatVector, v3: TFloatVector): SparseFloatVector

  def apply(v1: SparseLongKeyFloatVector, v2: TLongFloatVector, v3: TLongFloatVector): SparseLongKeyFloatVector
}

class DefaultUnary(var lr: Double, var isInplace: Boolean) extends Unary {
  override def apply(v1: DenseDoubleVector): DenseDoubleVector = {
    if (isInplace) {
      v1.timesBy(-lr).asInstanceOf[DenseDoubleVector]
    } else {
      v1.times(-lr).asInstanceOf[DenseDoubleVector]
    }
  }

  override def apply(v1: SparseDoubleVector): SparseDoubleVector = {
    if (isInplace) {
      v1.timesBy(-lr).asInstanceOf[SparseDoubleVector]
    } else {
      v1.times(-lr).asInstanceOf[SparseDoubleVector]
    }
  }

  override def apply(v1: SparseLongKeyDoubleVector): SparseLongKeyDoubleVector = {
    if (isInplace) {
      v1.timesBy(-lr).asInstanceOf[SparseLongKeyDoubleVector]
    } else {
      v1.times(-lr).asInstanceOf[SparseLongKeyDoubleVector]
    }
  }

  override def apply(v1: DenseFloatVector): DenseFloatVector = {
    if (isInplace) {
      v1.times(-lr).asInstanceOf[DenseFloatVector]
    } else {
      v1.timesBy(-lr).asInstanceOf[DenseFloatVector]
    }
  }

  override def apply(v1: SparseFloatVector): SparseFloatVector = {
    if (isInplace) {
      v1.timesBy(-lr).asInstanceOf[SparseFloatVector]
    } else {
      v1.times(-lr).asInstanceOf[SparseFloatVector]
    }
  }

  override def apply(v1: SparseLongKeyFloatVector): SparseLongKeyFloatVector = {
    if (isInplace) {
      v1.timesBy(-lr).asInstanceOf[SparseLongKeyFloatVector]
    } else {
      v1.times(-lr).asInstanceOf[SparseLongKeyFloatVector]
    }
  }
}

class NullUnary(var isInplace: Boolean) extends Unary {
  override def apply(v1: DenseDoubleVector): DenseDoubleVector = {
    if (isInplace) v1 else v1.clone()
  }

  override def apply(v1: SparseDoubleVector): SparseDoubleVector = {
    if (isInplace) v1 else v1.clone()
  }

  override def apply(v1: SparseLongKeyDoubleVector): SparseLongKeyDoubleVector = {
    if (isInplace) v1 else v1.clone()
  }

  override def apply(v1: DenseFloatVector): DenseFloatVector = {
    if (isInplace) v1 else v1.clone()
  }

  override def apply(v1: SparseFloatVector): SparseFloatVector = {
    if (isInplace) v1 else v1.clone()
  }

  override def apply(v1: SparseLongKeyFloatVector): SparseLongKeyFloatVector = {
    if (isInplace) v1 else v1.clone()
  }
}

class DefaultBinary(var lr: Double, var isInplace: Boolean) extends Binary {
  override def apply(v1: DenseDoubleVector, v2: TDoubleVector): DenseDoubleVector = {
    if (isInplace) {
      v1.timesBy(-lr).asInstanceOf[DenseDoubleVector]
    } else {
      v1.times(-lr).asInstanceOf[DenseDoubleVector]
    }
  }

  override def apply(v1: SparseDoubleVector, v2: TDoubleVector): SparseDoubleVector = {
    if (isInplace) {
      v1.timesBy(-lr).asInstanceOf[SparseDoubleVector]
    } else {
      v1.times(-lr).asInstanceOf[SparseDoubleVector]
    }
  }

  override def apply(v1: SparseLongKeyDoubleVector, v2: TLongDoubleVector): SparseLongKeyDoubleVector = {
    if (isInplace) {
      v1.timesBy(-lr).asInstanceOf[SparseLongKeyDoubleVector]
    } else {
      v1.times(-lr).asInstanceOf[SparseLongKeyDoubleVector]
    }
  }

  override def apply(v1: DenseFloatVector, v2: TFloatVector): DenseFloatVector = {
    if (isInplace) {
      v1.times(-lr).asInstanceOf[DenseFloatVector]
    } else {
      v1.timesBy(-lr).asInstanceOf[DenseFloatVector]
    }
  }

  override def apply(v1: SparseFloatVector, v2: TFloatVector): SparseFloatVector = {
    if (isInplace) {
      v1.timesBy(-lr).asInstanceOf[SparseFloatVector]
    } else {
      v1.times(-lr).asInstanceOf[SparseFloatVector]
    }
  }

  override def apply(v1: SparseLongKeyFloatVector, v2: TLongFloatVector): SparseLongKeyFloatVector = {
    if (isInplace) {
      v1.timesBy(-lr).asInstanceOf[SparseLongKeyFloatVector]
    } else {
      v1.times(-lr).asInstanceOf[SparseLongKeyFloatVector]
    }
  }
}

class NullBinary(var isInplace: Boolean) extends Binary {
  override def apply(v1: DenseDoubleVector, v2: TDoubleVector): DenseDoubleVector = {
    if (isInplace) v1 else v1.clone()
  }

  override def apply(v1: SparseDoubleVector, v2: TDoubleVector): SparseDoubleVector = {
    if (isInplace) v1 else v1.clone()
  }

  override def apply(v1: SparseLongKeyDoubleVector, v2: TLongDoubleVector): SparseLongKeyDoubleVector = {
    if (isInplace) v1 else v1.clone()
  }

  override def apply(v1: DenseFloatVector, v2: TFloatVector): DenseFloatVector = {
    if (isInplace) v1 else v1.clone()
  }

  override def apply(v1: SparseFloatVector, v2: TFloatVector): SparseFloatVector = {
    if (isInplace) v1 else v1.clone()
  }

  override def apply(v1: SparseLongKeyFloatVector, v2: TLongFloatVector): SparseLongKeyFloatVector = {
    if (isInplace) v1 else v1.clone()
  }
}

class DefaultTernary(var lr: Double, var isInplace: Boolean) extends Ternary {
  override def apply(v1: DenseDoubleVector, v2: TDoubleVector, v3: TDoubleVector): DenseDoubleVector = {
    if (isInplace) {
      v1.timesBy(-lr).asInstanceOf[DenseDoubleVector]
    } else {
      v1.times(-lr).asInstanceOf[DenseDoubleVector]
    }
  }

  override def apply(v1: SparseDoubleVector, v2: TDoubleVector, v3: TDoubleVector): SparseDoubleVector = {
    if (isInplace) {
      v1.timesBy(-lr).asInstanceOf[SparseDoubleVector]
    } else {
      v1.times(-lr).asInstanceOf[SparseDoubleVector]
    }
  }

  override def apply(v1: SparseLongKeyDoubleVector, v2: TLongDoubleVector, v3: TLongDoubleVector): SparseLongKeyDoubleVector = {
    if (isInplace) {
      v1.timesBy(-lr).asInstanceOf[SparseLongKeyDoubleVector]
    } else {
      v1.times(-lr).asInstanceOf[SparseLongKeyDoubleVector]
    }
  }

  override def apply(v1: DenseFloatVector, v2: TFloatVector, v3: TFloatVector): DenseFloatVector = {
    if (isInplace) {
      v1.timesBy(-lr).asInstanceOf[DenseFloatVector]
    } else {
      v1.times(-lr).asInstanceOf[DenseFloatVector]
    }
  }

  override def apply(v1: SparseFloatVector, v2: TFloatVector, v3: TFloatVector): SparseFloatVector = {
    if (isInplace) {
      v1.timesBy(-lr).asInstanceOf[SparseFloatVector]
    } else {
      v1.times(-lr).asInstanceOf[SparseFloatVector]
    }
  }

  override def apply(v1: SparseLongKeyFloatVector, v2: TLongFloatVector, v3: TLongFloatVector): SparseLongKeyFloatVector = {
    if (isInplace) {
      v1.timesBy(-lr).asInstanceOf[SparseLongKeyFloatVector]
    } else {
      v1.times(-lr).asInstanceOf[SparseLongKeyFloatVector]
    }
  }
}

class NullTernary(var isInplace: Boolean) extends Ternary {
  override def apply(v1: DenseDoubleVector, v2: TDoubleVector, v3: TDoubleVector): DenseDoubleVector = {
    if (isInplace) v1 else v1.clone()
  }

  override def apply(v1: SparseDoubleVector, v2: TDoubleVector, v3: TDoubleVector): SparseDoubleVector = {
    if (isInplace) v1 else v1.clone()
  }

  override def apply(v1: SparseLongKeyDoubleVector, v2: TLongDoubleVector, v3: TLongDoubleVector): SparseLongKeyDoubleVector = {
    if (isInplace) v1 else v1.clone()
  }

  override def apply(v1: DenseFloatVector, v2: TFloatVector, v3: TFloatVector): DenseFloatVector = {
    if (isInplace) v1 else v1.clone()
  }

  override def apply(v1: SparseFloatVector, v2: TFloatVector, v3: TFloatVector): SparseFloatVector = {
    if (isInplace) v1 else v1.clone()
  }

  override def apply(v1: SparseLongKeyFloatVector, v2: TLongFloatVector, v3: TLongFloatVector): SparseLongKeyFloatVector = {
    if (isInplace) v1 else v1.clone()
  }
}
