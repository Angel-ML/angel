package struct2vec.fastdtwUtils

/**
 * A "vector" of BigDecimal values
 *
 * @param v a varagrs sequence of big decimals making up the values for each dimension
 */
final case class VectorValue[T](v: T*) extends Seq[T] {
  override def length: Int = v.size

  override def iterator: Iterator[T] = v.iterator

  def apply(n: Int): T = {
    v(n)
  }

  /** Creates a copy of this vector with the value `v` at the specified `index`` */
  def withElement(index: Int, v: T): VectorValue[T] = {
    val r = index match {
      case 0 => VectorValue(v) ++ tail
      case x if x == (length - 1) => init :+ v
      case _ => (dropRight(length - index) :+ v) ++ drop(index + 1)
    }
    VectorValue(r: _*)
  }

}

object VectorValue {
  /** Creates an empty vector */
  def empty[T](size: Int, init: T): VectorValue[T] = {
    VectorValue(Seq.fill(size)(init): _*)
  }

}
