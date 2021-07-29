package com.tencent.angel.spark.ml.featureEngineering.RandomizedSVD

import java.util.Arrays
import breeze.linalg.{MatrixSingularException, inv, DenseMatrix => BDM, DenseVector => BDV}
import breeze.numerics.{sqrt => brzSqrt}
import com.github.fommil.netlib.ARPACK
import com.github.fommil.netlib.BLAS.{getInstance => NativeBLAS}
import com.tencent.angel.spark.ml.util._
import org.apache.spark.mllib.linalg.distributed.{IndexedRow, IndexedRowMatrix}
import org.apache.spark.mllib.linalg.{DenseMatrix, DenseVector, Matrices, Matrix, SingularValueDecomposition, SparseVector, Vector, Vectors => mllibVectors}
import org.apache.spark.mllib.random.StandardNormalGenerator
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row
import org.apache.spark.{SparkConf, SparkContext}
import org.netlib.util.{doubleW, intW}

/**
 * Created by isakjiang on 2020-11-16
 * paper url : https://arxiv.org/pdf/0909.4061.pdf
 * 算法根据数据稠密与稀疏类型分为两种：
 * (1):稠密，矩阵直接计算
 * (2):稀疏，矩阵采用新类型计算
 *
 */
object RandomizedSVDLocal {

  //奇异值个数
  val K = "k"
  //条件数倒数
  val RCOND = "rCond"
  //奇异值向量S的保存路径
  val OUTPUTS = "outputS"
  //特征值对应的特征向量的保存路径
  val OUTPUTV = "outputV"
  //左奇异向量U的保存路径,若为null则不计算矩阵U,否则计算矩阵U并保存
  val OUTPUTU = "outputU"
  //the way of iteration: "none" or "QR"
  val ITERATION_NORMALIZER = "iterationNormalizer"
  //q for overSample
  val Q_OVERSAMPLE = "qOverSample"
  //the number of iteration
  val NUM_ITERATION = "numIteration"
  val QR = "QR"
  val AUTO = "auto"
  val NONE = "none"

  def main(args: Array[String]) = {

    val mode = "local"
    val input = "data/mlDataTest/wine.data"
    val outputS = "data/output//outputS"
    val outputV = "data/output//outputV"
    val outputU = "data/output//outputU"
    //feature cols for matrix
    val featureCols = null
    //object col for index of matrix
    val labelCol = 0
    val sampleRate = 1
    val partitionNum = 1
    //算法参数
    val k = 1

    //parameters for randomized
    val iterationNormalizer = QR
    val qOverSample = 3
    val numIteration = AUTO
    //parameters for svd
    val rCond = 1e-9
    val sep = ","

    val sc = start(mode)
    run(sc, input, partitionNum, sampleRate, outputS, outputV, outputU, featureCols,
      labelCol, k, rCond, iterationNormalizer, qOverSample, numIteration, sep)
    stop()
  }


  def run(sc: SparkContext,
          input: String,
          partitionNum: Int,
          sampleRate: Double,
          outputS: String,
          outputV: String,
          outputU: String,
          featureCols: String,
          labelCol: Int,
          k: Int,
          rCond: Double,
          iterationNormalizer: String,
          qOverSample: Int,
          numIteration: String,
          sep: String
         ) {

    //decide whether it's sparse or dense type
    val isDenseType = if (HDFSUtils.isSparse(sc, input, sep)) false else true

    //whether to compute U
    val isComputeU = if (outputU == null) false else true

    //the index of matrix is on first column, and we should use transform function for large table
    val dataFrame = DataLoader.loadlabeled(sc, input, partitionNum, sampleRate, featureCols, labelCol, true, sep)

    //switch the data to indexedRowMatrix
    val rows = dataFrame.select(DFStruct.LABEL, DFStruct.FEATURE).rdd
      .map { case Row(label: Double, feature: org.apache.spark.ml.linalg.Vector) =>
        IndexedRow(label.toLong, mllibVectors.fromML(feature))
      }.cache()
    rows.name = "RSVDRows"

    //sample this matrix by Randomized SVD
    val mat: IndexedRowMatrix = new IndexedRowMatrix(rows)

    val matRow = mat.numRows
    val matCol = mat.numCols

    //create the random matrix
    val L = k + qOverSample
    //check the row, col and k,(k + qOverSample) <= min{mat.row, mat.col}
    if (L > Math.min(matRow, matCol))
      throw new Exception(s"not satisfy the condition:(k + qOverSample) <= min{matrixRow, matrixCol},but find " +
        s"k:$k , qOverSample:$qOverSample, matrixRow:$matRow, matrixCol:$matCol")

    //decide the number of iteration
    val actNumIter = if (numIteration == AUTO) {
      if (k < (Math.min(matRow, matCol) * 0.1)) 7 else 4
    } else
      numIteration.toInt

    if (isDenseType)
      denseProcess(sc, mat, matCol.toInt, L, isComputeU, k, rCond, iterationNormalizer, actNumIter, outputS, outputV, outputU, sep)
    else
      sparseProcess(sc, mat, matCol.toInt, L, isComputeU, k, rCond, iterationNormalizer, actNumIter, outputS, outputV, outputU, sep)

  }

  //dense computition process
  def denseProcess(sc: SparkContext,
                   mat: IndexedRowMatrix,
                   matCol: Int,
                   L: Int,
                   isComputeU: Boolean,
                   k: Int,
                   rCond: Double,
                   iterationNormalizer: String,
                   actNumIter: Int,
                   outputS: String,
                   outputV: String,
                   outputU: String,
                   sep: String) = {

    var sampleMat = createRandomMatrix(matCol, L)
    var tempIndexMat: IndexedRowMatrix = mat.multiply(sampleMat)

    val actualQ = iterationNormalizer match {
      case QR =>
        //QR for Q0,note : cache for test after a while
        tempIndexMat = tallSkinnyQR(tempIndexMat, true, L)._1
        (0 until actNumIter).map { i =>
          // A^T * Q,that is indexMatrix multiply indexMatrix
          sampleMat = multiplyRightBig(mat, tempIndexMat)
          //QR for matrix Q
          sampleMat = QR4Matrix(sampleMat, true)._1

          //A * Q,that is indexMatrix multiply matrix
          tempIndexMat = mat.multiply(sampleMat)
          //QR for IndexMatrix Q0
          tempIndexMat = tallSkinnyQR(tempIndexMat, true, L)._1
        }
        tempIndexMat

      case NONE =>
        (0 until actNumIter).map { i =>
          // A^T * Q,that is indexMatrix multiply indexMatrix
          sampleMat = multiplyRightBig(mat, tempIndexMat)
          //A * Q,that is indexMatrix multiply matrix
          tempIndexMat = mat.multiply(sampleMat)
        }
        //QR for Q
        tallSkinnyQR(tempIndexMat, true, L)._1
    }

    actualQ.rows.name = "QRows"

    //缓存Q,以计算左奇异向量
    if (isComputeU) {
      actualQ.rows.cache()
    }

    //B = Q^T * A
    val B = multiplyRightBig(actualQ, mat)

    //release the rdd in row
    mat.rows.unpersist(false)

    //construct for the indexedMatrix,start from 0
    var lineId: Long = 0
    val indexedB = (0 until B.numRows).map { i =>
      val item = (0 until B.numCols).map(j => B(i, j)).toArray
      val row = IndexedRow(lineId, mllibVectors.dense(item))
      lineId += 1
      row
    }

    val indexedBRdd = sc.makeRDD(indexedB, 10)
    val indexedRomMatB = new IndexedRowMatrix(indexedBRdd)

    //svd decompose
    val svd = indexedRomMatB.computeSVD(k, isComputeU, rCond)

    //保存S
    val sToString = svd.s.toArray.map(_.toString)
    val svdS = sc.makeRDD(Array(sToString))
    DataSaver.save(svdS, outputS, sep)

    //保存V
    val singVec = svd.V
    val singVecR = singVec.numRows
    val singVecC = singVec.numCols
    val singVecArray = (0 until singVecR).map { case i =>
      val rowArray = (0 until singVecC).map { case j => singVec(i, j).toString }.toArray
      val iArray = Array(i.toString)
      iArray ++ rowArray
    }
    val svdV = sc.makeRDD(singVecArray)
    DataSaver.save(svdV, outputV, sep)

    //保存U
    if (isComputeU) {
      //actual U for input matrix,that is U = Q * U~
      //according to the indexedLineId to sort the U,maybe this step can  be skipped
      val matU = svd.U.rows.sortBy(_.index).map(_.vector.toArray).collect
      val matURow = matU.length
      val matUCol = matU(0).length
      val matUFlat = matU.flatMap(x => x)
      val UDenseMat = new DenseMatrix(matURow, matUCol, matUFlat, true)
      val matUActual = actualQ.multiply(UDenseMat)
      //index与行向量组成数组
      val svdU = matUActual.rows.map { x =>
        val vecToArray = x.vector.toArray.map(_.toString)
        val indexToArray = Array(x.index.toString)
        indexToArray ++ vecToArray
      }
      DataSaver.save(svdU, outputU, sep)
      actualQ.rows.unpersist(false)
    }
  }

  //sparse computition process
  def sparseProcess(sc: SparkContext,
                    mat: IndexedRowMatrix,
                    matCol: Int,
                    L: Int,
                    isComputeU: Boolean,
                    k: Int,
                    rCond: Double,
                    iterationNormalizer: String,
                    actNumIter: Int,
                    outputS: String,
                    outputV: String,
                    outputU: String,
                    sep: String) = {

    var sampleMat = createRandomMatrix(matCol, L)
    var tempIndexMat: IndexedRowMatrix = mat.multiply(sampleMat)

    val actualQ = iterationNormalizer match {

      case QR =>
        tempIndexMat = tallSkinnyQR(tempIndexMat, true, L)._1
        (0 until actNumIter).map { i =>
          //A^T * Q,that is (Q^T * A)^T
          sampleMat = multiplySparseTranspose(tempIndexMat, mat, L, matCol.toInt).transpose
          sampleMat = QR4Matrix(sampleMat, true)._1

          //A * matrix
          tempIndexMat = mat.multiply(sampleMat)
          tempIndexMat = tallSkinnyQR(tempIndexMat, true, L)._1
        }
        tempIndexMat

      case NONE =>
        //多个A*A^T的过程
        (0 until actNumIter).map { i =>
          //A^T * Q,that is (Q^T * A)^T
          sampleMat = multiplySparseTranspose(tempIndexMat, mat, L, matCol.toInt).transpose
          //A*matrix
          tempIndexMat = mat.multiply(sampleMat)
        }

        //增加QR分解过程
        tallSkinnyQR(tempIndexMat, true, L)._1
    }

    actualQ.rows.name = "QRows"

    //缓存Q,以计算左奇异向量
    if (isComputeU) {
      actualQ.rows.cache()
    }

    //qrQ^T * mat，本来是L * n,为了方便计算，对这个矩阵进行了转置,(1)
    val B = multiplySparseTranspose(actualQ, mat, L, matCol.toInt).transpose

    mat.rows.unpersist(false)

    //construct for the indexedMatrix,start from 0
    var lineId: Long = 0
    val indexedB = (0 until B.numRows).map { i =>
      val item = (0 until B.numCols).map(j => B(i, j)).toArray
      val row = IndexedRow(lineId, mllibVectors.dense(item))
      lineId += 1
      row
    }

    val indexedBRdd = sc.makeRDD(indexedB, 20)
    val indexedRomMatB = new IndexedRowMatrix(indexedBRdd)

    //L:矩阵的列数,(2)
    val svd = updateSVD(indexedRomMatB, L, k, true, rCond)

    //保存S
    val sToString = svd.s.toArray.map(_.toString)
    val svdS = sc.makeRDD(Array(sToString))
    DataSaver.save(svdS, outputS, sep)

    //保存U
    if (isComputeU) {
      val singVec = svd.V
      val actualU = actualQ.multiply(singVec)
      val UArray = actualU.rows.map { row =>
        Array(row.index.toString) ++ row.vector.toArray.map(_.toString)
      }
      DataSaver.save(UArray, outputU, sep)
      actualQ.rows.unpersist(false)
    }

    //保存V
    val matV = svd.U.rows.map(row => Array(row.index.toString) ++ row.vector.toArray.map(_.toString))
    DataSaver.save(matV, outputV, sep)
  }

  //create a Gaussian distribution matrix
  def createRandomMatrix(rowLeg: Int, colLeg: Int): Matrix = {
    val random = new StandardNormalGenerator
    random.setSeed(0)
    val newArray = new Array[Double](rowLeg * colLeg)
    (0 until newArray.length).map(i => newArray(i) = random.nextValue())
    Matrices.dense(rowLeg, colLeg, newArray)
  }

  //for A3*m * Bm*3,all stored as Am*3,Bm*3,actually is the (transform of the A)*B
  //transform the shortFat matrix by flatmap,each element in rdd represent one text in matrix,RDD[(Int,Array[Double])]
  def multiplyRightBig(shortFatMat: IndexedRowMatrix, indexRowMat: IndexedRowMatrix): Matrix = {
    //transform the shrotFat matrix
    val shortFat = shortFatMat.rows.map(x => (x.index, x.vector.toArray))
    val bigMat = indexRowMat.rows.map(x => (x.index, x.vector.toArray))
    val oneEleRdd = shortFat.flatMap { case (key, oneArray) =>
      (0 until oneArray.length).map(j => (key, (j, oneArray(j))))
    }

    val joinRdd = oneEleRdd
      .join(bigMat)
      .map { case (key: Long, (fat: (Int, Double), big: Array[Double])) =>
        val mulArray = big.map(ele => fat._2 * ele)
        (fat._1, mulArray)
      }

    val reduceResult = joinRdd.reduceByKey { case (x1: Array[Double], x2: Array[Double]) =>
      (0 until x1.length).map { i =>
        x1(i) + x2(i)
      }.toArray
    }.collect.sortBy(_._1)

    //program the result,actually this is a local val,switch it to matrix
    val rowLeg = reduceResult.length
    val colLeg = reduceResult(0)._2.length
    val arrayResult = reduceResult.flatMap(x => x._2)
    new DenseMatrix(rowLeg, colLeg, arrayResult, true)
  }

  def QR4Matrix(mat: Matrix, computeQ: Boolean = false): (Matrix, Matrix) = {

    val bdm = BDM.zeros[Double](mat.numRows, mat.numCols)
    (0 until mat.numRows).map { iRow =>
      val item = (0 until mat.numCols).map(jCol => mat(iRow, jCol)).toArray
      bdm(iRow, ::) := BDV(item).t
    }

    val matR = breeze.linalg.qr.reduced(bdm).r

    val finalR = Matrices.dense(matR.rows, matR.cols, matR.toArray)

    val finalQ = if (computeQ) {
      try {
        val invR = inv(matR)
        val invRMat = new DenseMatrix(invR.rows, invR.cols, invR.toArray)
        mat.multiply(invRMat)
      } catch {
        case err: MatrixSingularException =>
          throw new Exception("R is not invertible and return Q as null")
          null
      }
    } else {
      null
    }
    (finalQ, finalR)
  }

  //QR分解过程
  def tallSkinnyQR(mat: IndexedRowMatrix, computeQ: Boolean = false, matColN: Int): (IndexedRowMatrix, Matrix) = {
    val col = matColN
    // split rows horizontally into smaller matrices, and compute QR for each of them
    val blockQRs = mat.rows.map(it => it.vector).glom().filter(_.length != 0).map { partRows =>
      val bdm = BDM.zeros[Double](partRows.length, col)
      var i = 0
      partRows.foreach { row =>
        bdm(i, ::) := BDV(row.toArray).t
        i += 1
      }
      val r0 = breeze.linalg.qr.reduced(bdm).r
      //      println("r0的行数与列数：" + r0.rows + " " + r0.cols)
      r0
    }

    // combine the R part from previous results vertically into a tall matrix
    val combinedR = blockQRs.treeReduce({ (r1, r2) =>
      val stackedR = BDM.vertcat(r1, r2)
      breeze.linalg.qr.reduced(stackedR).r
    }, 4)

    val finalR = Matrices.dense(combinedR.rows, combinedR.cols, combinedR.toArray)

    val finalQ = if (computeQ) {
      try {
        val invR = inv(combinedR)
        val invRMat = Matrices.dense(invR.rows, invR.cols, invR.toArray)
        mat.multiply(invRMat)
      } catch {
        case err: MatrixSingularException =>
          throw new Exception("R is not invertible and return Q as null")
          null
      }
    } else {
      null
    }
    (finalQ, finalR)
  }

  def updateSVD(indexedMat: IndexedRowMatrix,
                colNum: Int,
                k: Int,
                computeU: Boolean,
                rCond: Double): SingularValueDecomposition[IndexedRowMatrix, Matrix] = {
    val tol = 1e-10
    val maxIter = math.max(300, k * 3)
    val matVec = indexedMat.rows.map(_.vector) //.map(a=>scala.Vector(a.toArray))
    // reconstruct gramian matrix by reduce 4 times to  save memory
    val G0 = computeReGramianMatrix(matVec, colNum)

    val G = new BDM[Double](G0.numCols, G0.numRows, G0.toArray)
    //    val brzSvd.SVD(uFull: BDM[Double], sigmaSquaresFull: BDV[Double], _) = brzSvd(G)
    val (sigmaSquares: BDV[Double], u: BDM[Double]) = symmetricEigs(v => G * v, colNum, k, tol, maxIter)

    val sigmas: BDV[Double] = brzSqrt(sigmaSquares)

    // Determine the effective rank.
    val sigma0 = sigmas(0)
    val threshold = rCond * sigma0
    var i = 0
    // sigmas might have a length smaller than k, if some Ritz values do not satisfy the convergence
    // criterion specified by tol after max number of iterations.
    // Thus use i < min(k, sigmas.length) instead of i < k.
    if (sigmas.length < k) {
      LogUtils.logTime(s"Requested $k singular values but only found ${sigmas.length} converged.")
    }

    //开始迭代
    while (i < math.min(k, sigmas.length) && sigmas(i) >= threshold) {
      i += 1
    }
    val sk = i

    if (sk < k) {
      LogUtils.logTime(s"Requested $k singular values but only found $sk nonzeros.")
    }

    val s = mllibVectors.dense(Arrays.copyOfRange(sigmas.data, 0, sk))
    val V = Matrices.dense(colNum, sk, Arrays.copyOfRange(u.data, 0, colNum * sk))

    if (computeU) {
      // N = Vk * Sk^{-1}
      val N = new BDM[Double](colNum, sk, Arrays.copyOfRange(u.data, 0, colNum * sk))
      var i = 0
      var j = 0
      while (j < sk) {
        i = 0
        val sigma = sigmas(j)
        while (i < colNum) {
          N(i, j) /= sigma
          i += 1
        }
        j += 1
      }
      val UMat = Matrices.dense(N.rows, N.cols, N.toArray)
      val U = indexedMat.multiply(UMat)
      SingularValueDecomposition(U, s, V)
    } else {
      SingularValueDecomposition(null, s, V)
    }

  }

  //A^T * A
  def computeReGramianMatrix(rows: RDD[Vector], numCols: Int): Matrix = {
    val n = numCols.toInt
    // Computes n*(n+1)/2, avoiding overflow in the multiplication.
    // This succeeds when n <= 65535, which is checked above
    val nt: Int = if (n % 2 == 0) ((n / 2) * (n + 1)) else (n * ((n + 1) / 2))

    // Compute the upper triangular part of the gram matrix.
    val GU = rows.treeAggregate(new BDV[Double](new Array[Double](nt)))(
      seqOp = (U, v) => {
        spr(1.0, v, U.data)
        U
      }, combOp = (U1, U2) => U1 += U2, 4)

    triuToFull(n, GU.data)
  }

  def spr(alpha: Double, v: Vector, U: Array[Double]): Unit = {
    val n = v.size
    v match {
      case DenseVector(values) =>
        NativeBLAS.dspr("U", n, alpha, values, 1, U)
      case SparseVector(size, indices, values) =>
        val nnz = indices.length
        var colStartIdx = 0
        var prevCol = 0
        var col = 0
        var j = 0
        var i = 0
        var av = 0.0
        while (j < nnz) {
          col = indices(j)
          // Skip empty columns.
          colStartIdx += (col - prevCol) * (col + prevCol + 1) / 2
          col = indices(j)
          av = alpha * values(j)
          i = 0
          while (i <= j) {
            U(colStartIdx + indices(i)) += av * values(i)
            i += 1
          }
          j += 1
          prevCol = col
        }
    }
  }

  private def triuToFull(n: Int, U: Array[Double]): Matrix = {
    val G = new BDM[Double](n, n)

    var row = 0
    var col = 0
    var idx = 0
    var value = 0.0
    while (col < n) {
      row = 0
      while (row < col) {
        value = U(idx)
        G(row, col) = value
        G(col, row) = value
        idx += 1
        row += 1
      }
      G(col, col) = U(idx)
      idx += 1
      col += 1
    }

    Matrices.dense(n, n, G.data)
  }

  def symmetricEigs(
                     mul: BDV[Double] => BDV[Double],
                     n: Int,
                     k: Int,
                     tol: Double,
                     maxIterations: Int): (BDV[Double], BDM[Double]) = {
    // TODO: remove this function and use eigs in breeze when switching breeze version
    require(n > k, s"Number of required eigenvalues $k must be smaller than matrix dimension $n")

    val arpack = ARPACK.getInstance()

    // tolerance used in stopping criterion
    val tolW = new doubleW(tol)
    // number of desired eigenvalues, 0 < nev < n
    val nev = new intW(k)
    // nev Lanczos vectors are generated in the first iteration
    // ncv-nev Lanczos vectors are generated in each subsequent iteration
    // ncv must be smaller than n
    val ncv = math.min(2 * k, n)

    // "I" for standard eigenvalue problem, "G" for generalized eigenvalue problem
    val bmat = "I"
    // "LM" : compute the NEV largest (in magnitude) eigenvalues
    val which = "LM"

    var iparam = new Array[Int](11)
    // use exact shift in each iteration
    iparam(0) = 1
    // maximum number of Arnoldi update iterations, or the actual number of iterations on output
    iparam(2) = maxIterations
    // Mode 1: A*x = lambda*x, A symmetric
    iparam(6) = 1

    require(n * ncv.toLong <= Integer.MAX_VALUE && ncv * (ncv.toLong + 8) <= Integer.MAX_VALUE,
      s"k = $k and/or n = $n are too large to compute an eigendecomposition")

    var ido = new intW(0)
    var info = new intW(0)
    var resid = new Array[Double](n)
    var v = new Array[Double](n * ncv)
    var workd = new Array[Double](n * 3)
    var workl = new Array[Double](ncv * (ncv + 8))
    var ipntr = new Array[Int](11)

    // call ARPACK's reverse communication, first iteration with ido = 0
    arpack.dsaupd(ido, bmat, n, which, nev.`val`, tolW, resid, ncv, v, n, iparam, ipntr, workd,
      workl, workl.length, info)

    val w = BDV(workd)

    // ido = 99 : done flag in reverse communication
    while (ido.`val` != 99) {
      if (ido.`val` != -1 && ido.`val` != 1) {
        throw new IllegalStateException("ARPACK returns ido = " + ido.`val` +
          " This flag is not compatible with Mode 1: A*x = lambda*x, A symmetric.")
      }
      // multiply working vector with the matrix
      val inputOffset = ipntr(0) - 1
      val outputOffset = ipntr(1) - 1
      val x = w.slice(inputOffset, inputOffset + n)
      val y = w.slice(outputOffset, outputOffset + n)
      y := mul(x)
      // call ARPACK's reverse communication
      arpack.dsaupd(ido, bmat, n, which, nev.`val`, tolW, resid, ncv, v, n, iparam, ipntr,
        workd, workl, workl.length, info)
    }

    if (info.`val` != 0) {
      info.`val` match {
        case 1 => throw new IllegalStateException("ARPACK returns non-zero info = " + info.`val` +
          " Maximum number of iterations taken. (Refer ARPACK user guide for details)")
        case 3 => throw new IllegalStateException("ARPACK returns non-zero info = " + info.`val` +
          " No shifts could be applied. Try to increase NCV. " +
          "(Refer ARPACK user guide for details)")
        case _ => throw new IllegalStateException("ARPACK returns non-zero info = " + info.`val` +
          " Please refer ARPACK user guide for error message.")
      }
    }

    val d = new Array[Double](nev.`val`)
    val select = new Array[Boolean](ncv)
    // copy the Ritz vectors
    val z = java.util.Arrays.copyOfRange(v, 0, nev.`val` * n)

    // call ARPACK's post-processing for eigenvectors
    arpack.dseupd(true, "A", select, d, z, n, 0.0, bmat, n, which, nev, tol, resid, ncv, v, n,
      iparam, ipntr, workd, workl, workl.length, info)

    // number of computed eigenvalues, might be smaller than k
    val computed = iparam(4)

    val eigenPairs = java.util.Arrays.copyOfRange(d, 0, computed).zipWithIndex.map { r =>
      (r._1, java.util.Arrays.copyOfRange(z, r._2 * n, r._2 * n + n))
    }

    // sort the eigen-pairs in descending order
    val sortedEigenPairs = eigenPairs.sortBy(-_._1)

    // copy eigenvectors in descending order of eigenvalues
    val sortedU = BDM.zeros[Double](n, computed)
    sortedEigenPairs.zipWithIndex.foreach { r =>
      val b = r._2 * n
      var i = 0
      while (i < n) {
        sortedU.data(b + i) = r._1._2(i)
        i += 1
      }
    }

    (BDV[Double](sortedEigenPairs.map(_._1)), sortedU)
  }

  //两个矩阵都进行转置，使得两者可以实现本地点乘
  //K:sampleMat的列（L）,n:sparseMat的列, 均为未转置之前
  def multiplySparseTranspose(sampleMat: IndexedRowMatrix, sparseMat: IndexedRowMatrix, K: Int, n: Int): Matrix = {
    //transpose the qMat
    val qMat = sampleMat.rows.flatMap { row =>
      val row2Array = row.vector.toArray
      row2Array.indices.map(idCol => (idCol, (row.index, row2Array(idCol))))
    }.groupByKey(K).map { colGroup =>
      val tempArray = colGroup._2.toArray.sortBy(_._1).map(_._2)
      (colGroup._1, tempArray)
    }

    val tempIndexBlockMat = sparseMat.toBlockMatrix(1000, 1000)
    val aMat = tempIndexBlockMat.transpose.toIndexedRowMatrix

    val firstLocalSpa = aMat.rows.collect()
    //每一行都与sparse矩阵相乘
    val result = qMat.map { row =>
      val qArray = row._2
      val allDot = firstLocalSpa.map { indexRow =>
        val indexV = indexRow.vector.toSparse
        //bug is here
        val id = indexV.indices
        val dot = (0 until id.length).map { i => qArray(id(i)) * indexV.values(i) }.toArray.reduce(_ + _)
        (indexRow.index, dot)
      }.sortBy(_._1)
      (row._1, allDot)
    }.collect()

    //按rowwise排序
    val orderedResult = result.sortBy(_._1).flatMap { it => it._2.map(_._2) }
    new DenseMatrix(K, n, orderedResult, true)
  }

  def start(mode: String = "local"): SparkContext = {
    val conf = new SparkConf()
    conf.setMaster(mode)
    conf.setAppName(this.getClass.getSimpleName)
    val sc = new SparkContext(conf)
    sc.setLogLevel("WARN")
    sc
  }

  def stop(): Unit = {
    SparkContext.getOrCreate().stop()
  }

}
