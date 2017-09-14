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
 * Unless required by applicable law or agreed to in writing, software distributed under the
 * License is
 * distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied. See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.tencent.angel.ml.toolkits.modelconverter

import java.io.IOException
import java.nio.ByteBuffer
import java.util
import java.util.concurrent._
import java.util.concurrent.atomic.AtomicBoolean

import com.google.common.util.concurrent.ThreadFactoryBuilder
import com.tencent.angel.conf.AngelConf
import com.tencent.angel.ml.conf.MLConf
import org.apache.commons.logging.LogFactory
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FSDataInputStream, FSDataOutputStream, FileStatus, Path}

/**
 * This module converts the binary output of the PS to plain text
 */
class ModelConverter(val conf: Configuration) {
  val LOG = LogFactory.getLog(classOf[ModelConverter])

  // Name of PSModel
  val modelName: String = conf.get(MLConf.ML_MODEL_NAME)
  // Thread number to conver model to text
  val threadNum: Int = conf.getInt(MLConf.ML_MODEL_CONVERT_THREAD_COUNT, MLConf
    .DEFAULT_ML_MODEL_CONVERT_THREAD_COUNT)

  // Input binary model file path
  val inPath = new Path(conf.get(AngelConf.ANGEL_TRAIN_DATA_PATH))
  val inFS = inPath.getFileSystem(conf)
  val inFStatus = new Path(conf.get(AngelConf.ANGEL_TRAIN_DATA_PATH)).getFileSystem(conf).listStatus(inPath)

  // Output plain text model file path
  val outPath = new Path(conf.get(MLConf.ML_MODEL_OUT_PATH))
  val outFS = outPath.getFileSystem(conf)
  if (outFS.exists(outPath)) {
    outFS.delete(outPath)
    LOG.info("Output path already exist, delete: " + outPath)
  }
  outFS.mkdirs(outPath)

  var parserPool = Executors.newFixedThreadPool(threadNum, new ThreadFactoryBuilder()
  .setNameFormat("parser").build())
  val parserThread = new util.ArrayList[ModelConverter#Parser]

  /**
   * converts the binary output of the PS to plain text
   */
  def parse( ) = {

    for (i <- 0 until inFStatus.length) {
      val stat = inFStatus(i)
      val out = outFS.create(new Path(outPath, String.valueOf(i)))
      val parser = new Parser(stat, out)

      parserThread.add(parser)
      parserPool.execute(parser)
    }

    var convertSuccess = true

    for (i <- 0 until parserThread.size()) {
      val parser = parserThread.get(i)

      while (!parser.finishFlag.get) {
        Thread.sleep(1000)
      }

      if (! parser.isSuccess.get)  {
        convertSuccess = false
        LOG.error("convert failed for " + parser.getRrrLog)
      }

      parserPool.shutdownNow
    }

    if (!convertSuccess)
      LOG.error("convert failed.")
  }

  /**
    * Converts the binary output of the PS to plain text
    */
  class Parser(val status: FileStatus, val out: FSDataOutputStream) extends Runnable {
    val isSuccess = new AtomicBoolean(false)
    val finishFlag = new AtomicBoolean(false)
    val errorLog: String = null

    override
    def run() {
      LOG.info("open file " + status.getPath)

      try {
        var fin = inFS.open(status.getPath)

        // read partition info
        val matId = fin.readInt
        val partSize = fin.readInt
        val startRow = fin.readInt
        val startCol = fin.readInt
        val endRow = fin.readLong
        val endCol = fin.readLong
        val rowType = fin.readUTF

        val patInfo = "Partition info: matrixID=" + matId + " partSize=" + partSize + " rowType=" +
          rowType + ", " + "partition " + "range from[" + startRow +
          ", " + startCol + "] to [" + endRow + ", " + endCol + "]"
        LOG.info(patInfo)

        rowType match {
          case "T_DOUBLE_SPARSE" => parseDoubleSparse(fin)

          case "T_DOUBLE_DENSE" => parseDoubleDense(fin, startCol, endCol)

          case "T_FLOAT_DENSE" => parseFloatDense(fin, startCol, endCol)

          case "T_INT_DENSE" => parseIntDense(fin, startCol, endCol)

          case "T_INT_SPARSE" => parseIntSparse(fin)

          case "T_INT_ARBITRARY" => parseArbitrary(fin, startCol, endCol)

        }
        fin.close()
        out.close()
        isSuccess.set(true)
        finishFlag.set(true)

    } catch{
        case e: IOException => LOG.error("Convert part file" + status.toString + " error", e)
        isSuccess.set(false)
      } finally {
        finishFlag.set(true)
        val end = System.currentTimeMillis()
      }

    }

    /**
      * Convert T_DOUBLE_SPARSE type files
      */
    def parseDoubleSparse(fin: FSDataInputStream): Unit = {
      val rowNum = fin.readInt

      var key = 0
      var value = .0

      for (j <- 0 until rowNum) {
        // TODO check
        val rowIndex = fin.readInt
        val clock = fin.readInt
        val rowLen = fin.readInt
        out.writeBytes(rowIndex + ", " + clock + "\n")
        LOG.info("Row info:  rowId:" + rowIndex + " clock:" + clock + " size:" + rowLen + "\n")

        for (k: Int <- 0 until rowLen) {
          key = fin.readInt
          value = fin.readDouble
          out.writeBytes( key + ":" + value + "\n")
        }
      }

      def getErrorLog: String = errorLog
    }

    /**
      * Convert T_DOUBLE_DENSE type files
      */
    def parseDoubleDense(fin: FSDataInputStream, startCol: Int, endCol: Long) {
      val rowLen = endCol.toInt - startCol
      val data = new Array[Byte](8 * (endCol.toInt - startCol))

      val rowNum = fin.readInt
      for (j <- 0 until rowNum) {
        val rowIndex = fin.readInt
        val clock = fin.readInt
        out.writeBytes(rowIndex + ", " + clock + "\n")
        LOG.info("Row info: rowId:" + rowIndex + " clock:" + clock + " len:" + rowLen + "\n")

        fin.readFully(data, 0, data.length)
        val dBuffer = ByteBuffer.wrap(data, 0, data.length).asDoubleBuffer

        for (k <- 0 until rowLen) {
          val value = dBuffer.get
          out.writeBytes((startCol + k) + ":" + value + "\n")
        }
      }
    }

    /**
      * Convert T_FLOAT_DENSE type files
      */
    def parseFloatDense(fin: FSDataInputStream, startCol: Int, endCol: Long) {
      val rowLen = endCol.toInt - startCol
      val data = new Array[Byte](4 * (endCol.toInt - startCol))

      val rowNum = fin.readInt
      for (j <- 0 until rowNum) {
        val rowIndex = fin.readInt
        val clock = fin.readInt
        out.writeBytes(rowIndex + ", " + clock + "\n")
        LOG.info("Row info: rowId:" + rowIndex + " clock:" + clock + " len:" + rowLen + "\n")

        fin.readFully(data, 0, data.length)
        val fBuffer = ByteBuffer.wrap(data, 0, data.length).asFloatBuffer

        for (k <- 0 until rowLen) {
          val value = fBuffer.get
          out.writeBytes((startCol + k) + ":" + value + "\n")
        }
      }
    }

    /**
      * Convert T_INT_SPARSE type files
      */
    def parseIntSparse(fin: FSDataInputStream): Unit = {
      val rowNum = fin.readInt

      for (j <- 0 until rowNum) {
        val rowIndex = fin.readInt
        val clock = fin.readInt
        val rowLen = fin.readInt

        out.writeBytes(rowIndex + ", " + clock + "\n")
        LOG.info("Row info: rowId:" + rowIndex + " clock:" + clock + " len:" + rowLen + "\n")

        for (k <- 0 until rowLen) {
          val key = fin.readInt
          val value = fin.readInt
          out.writeBytes(key + ":" + value + "\n")
        }
      }
    }

    /**
      * Convert T_INT_DENSE type files
      */
    def parseIntDense(fin: FSDataInputStream, startCol: Int, endCol: Long): Unit = {
      val rowNum = fin.readInt
      val data = new Array[Byte](4 * (endCol.toInt - startCol))
      val rowLen = endCol.toInt - startCol

      for (j <- 0 until rowNum) {
        val rowIndex = fin.readInt
        val clock = fin.readInt
        out.writeBytes(rowIndex + ", " + clock + "\n")
        LOG.info("Row info: rowId:" + rowIndex + " clock:" + clock + " len:" + rowLen + "\n")

        fin.readFully(data, 0, data.length)
        val iBuffer = ByteBuffer.wrap(data, 0, data.length).asIntBuffer

        for (k <- 0 until rowLen) {
          val value = iBuffer.get
          out.writeBytes((startCol + k) + ":" + value + "\n")
        }
      }
    }

    /**
      * Convert Arbitrary type files
      */
    def parseArbitrary(fin: FSDataInputStream, startCol: Int, endCol: Long): Unit = {
      val rowNum = fin.readInt()
      val data = new Array[Byte](4 * (endCol.toInt - startCol))
      for (j <- 0 until rowNum) {
        val rowIndex = fin.readInt
        val clock = fin.readInt
        out.writeBytes("rowID=" + rowIndex + ", clock=" + clock + "\n")
        LOG.info("Row info: rowId:" + rowIndex + " clock:" + clock + "\n")


        val denseOrSparse = fin.readUTF

        fin.readUTF() match {
          case "T_INT_DENSE" => parseArbitratyIntDense(fin, startCol, endCol)
          case "T_INT_SPARSE" => parseArbitratyIntSparse(fin)
        }
      }
    }

    def parseArbitratyIntDense(fin: FSDataInputStream, startCol: Int, endCol: Long): Unit= {
      val rowLen = endCol.toInt - startCol
      val data = new Array[Byte](4 * (endCol.toInt - startCol))

      fin.readFully(data, 0, data.length)
      val iBuffer = ByteBuffer.wrap(data, 0, data.length).asIntBuffer
      for (k <- 0 until rowLen) {
        val value = iBuffer.get()
        out.writeBytes((startCol + k) + ":" + value + "\n")
      }
    }

    def parseArbitratyIntSparse(fin: FSDataInputStream): Unit = {
      val nnz = fin.readInt
      val rowLen = fin.readInt

      for (k <- 0 until rowLen) {
        val key = fin.readInt
        val value = fin.readInt
        out.writeBytes(key + ":" + value + "\n")
      }
    }

    def getRrrLog = errorLog
  }
}