package com.tencent.angel.spark

import org.apache.spark.SparkException

import com.tencent.angel.spark.vector.{BreezePSVector, LocalPSVector, RemotePSVector}

/**
 * PSVectorProxy is a proxy of PSVector stored on the PS nodes.
 * We recommend to use PSVectorProxy type as parameter when programming, and call `mkLocal`,
 * `mkRemote` and `mkBreeze` to obtain LocalPSVector, RemotePSVector and BreezePSVector.
 *
 * LocalPSVector, RemotePSVector and BreezePSVector have implement a set of operations for
 * difference situation.
 */
class PSVectorProxy private[spark](
    @transient private var _pool: PSVectorPool,
    private[spark] val poolId: Int,
    private[spark] val id: Int,
    val numDimensions: Int) extends Serializable {

  @transient private var deleted = false

  private def pool = {
    if (_pool == null) {
      _pool = PSClient.get.getPool(poolId)
    }
    _pool
  }

  /**
   * Return PSVectorPool which this PSVector belongs to.
   * It only can be called on driver.
   */
  def getPool(): PSVectorPool = {
    PSClient.assertOnDriver()
    assertValid()
    pool
  }

  /**
   * Generate a LocalPSVector for this PSVectorKey
   */
  def mkLocal(): LocalPSVector = {
    assertValid()
    new LocalPSVector(this)
  }

  /**
   * Generate a RemotePSVector for this PSVectorKey
   */
  def mkRemote(): RemotePSVector = {
    assertValid()
    new RemotePSVector(this)
  }

  /**
   * Generate a BreezePSVector for this PSVectorKey
   */
  def mkBreeze(): BreezePSVector = {
    assertValid()
    new BreezePSVector(this)
  }

  /**
   * Delete this PSVector, PSVectorPool will recycle its space in pool
   */
  def delete(): Unit = {
    PSClient.assertOnDriver()
    if (!deleted) {
      pool.delete(this)
      deleted = true
    }
  }

  private[spark] def assertCompatible(other: PSVectorProxy): Unit = {
    if (this.poolId != other.poolId) {
      throw new SparkException("BLAS operators can only " +
        "be performed on vectors of the same pool!")
    }
  }

  private[spark] def assertCompatible(other: Array[Double]): Unit = {
    if (this.numDimensions != other.length) {
      throw new SparkException(s"The target array's dimension " +
        s"does not match this vector pool! \n" +
        s"pool dimension is $numDimensions," +
        s"but target array's dimension is ${other.length}")
    }
  }

  private[spark] def assertValid(): Unit = {
    if (deleted) {
      throw new SparkException("This vector has been deleted!")
    }
  }

  override def equals(other: Any): Boolean = {
    other match {
      case k: PSVectorProxy =>
        this.poolId == k.poolId && this.id == k.id
      case _ => false
    }
  }

  override def hashCode(): Int = {
    poolId * 31 + id
  }

}
