package com.tencent.angel.ml.matrix.psf.update

import com.tencent.angel.ml.matrix.psf.update.enhance.MMUpdateParam

class XavierNormal(param: MMUpdateParam) extends RandomNormal(param) {

  def this(matrixId: Int, rowId: Int, gain: Double, fin: Long, fout: Long) =
    this(new MMUpdateParam(matrixId, Array[Int](rowId),
      Array[Double](0.0, gain * math.sqrt(2.0 / (fin + fout)))))


  def this(matrixId: Int, startId: Int, length: Int, gain: Double, fin: Long, fout: Long) =
    this(new MMUpdateParam(matrixId, startId, length,
      Array[Double](0.0, gain * math.sqrt(2.0 / (fin + fout)))))

}
