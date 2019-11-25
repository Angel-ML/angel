/*
 * Tencent is pleased to support the open source community by making Angel available.
 *
 * Copyright (C) 2017-2018 THL A29 Limited, a Tencent company. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in
 * compliance with the License. You may obtain a copy of the License at
 *
 * https://opensource.org/licenses/Apache-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 *
 */

package com.tencent.angel.spark.ml.graph.node2vec


import com.tencent.angel.graph.client.node2vec.data.WalkPath
import com.tencent.angel.ml.matrix.{MatrixContext, RowType}
import com.tencent.angel.model.{MatrixLoadContext, MatrixSaveContext, ModelLoadContext, ModelSaveContext}
import com.tencent.angel.spark.context.PSContext
import com.tencent.angel.spark.ml.graph.params.HasMaxNodeId
import com.tencent.angel.spark.ml.util.LogUtils
import com.tencent.angel.spark.models.PSMatrix
import org.apache.hadoop.fs.Path
import org.apache.spark.SparkContext
import org.apache.spark.ml.Model
import org.apache.spark.ml.compat.Compat
import org.apache.spark.ml.param.ParamMap
import org.apache.spark.ml.util.{MLReadable, MLReader, MLWritable, MLWriter}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}


class Node2VecModel(override val uid: String) extends Model[Node2VecModel]
  with Node2VecParams with HasMaxNodeId with MLWritable {
  var walkPath: PSMatrix = _

  def this(uid: String, walkPath: PSMatrix) {
    this(uid)
    this.walkPath = walkPath
  }

  override def copy(extra: ParamMap): Node2VecModel = defaultCopy(extra)

  override def transform(dataset: Dataset[_]): DataFrame = {
    LogUtils.logTime("node2vec do not support transform, just return the input dataset")
    dataset.toDF()
  }

  override def transformSchema(schema: StructType): StructType = {
    LogUtils.logTime("node2vec do not support transformSchema, just return the input schema")
    schema
  }

  override def write: MLWriter = new Node2VecModel.Node2VecModelWriter(this)
}

object Node2VecModel extends MLReadable[Node2VecModel] {

  private case class Data(name: String, maxNodeId: Long, psPartitionNum: Int)

  private def deleteIfExists(modelPath: String, ss: SparkSession): Unit = {
    val path = new Path(modelPath)
    val fs = path.getFileSystem(ss.sparkContext.hadoopConfiguration)
    if (fs.exists(path)) {
      fs.delete(path, true)
    }
  }

  private[Node2VecModel] class Node2VecModelWriter(instance: Node2VecModel) extends MLWriter {
    override protected def saveImpl(path: String): Unit = {
      Compat.defaultParamsWriter.saveMetadata(instance, path, sc)

      val dataPathDir = new Path(path, "data").toString
      val data = Data(instance.walkPath.name, instance.getMaxNodeId, instance.getPSPartitionNum)
      sparkSession.createDataFrame(Seq(data)).repartition(1).write.parquet(dataPathDir)

      val walkPathDir = new Path(path, "walk").toString
      deleteIfExists(walkPathDir, sparkSession)
      val saveContext: ModelSaveContext = new ModelSaveContext(walkPathDir)
      saveContext.addMatrix(new MatrixSaveContext(instance.walkPath.name,
        classOf[Node2VecOutputFormat].getTypeName))
      PSContext.instance().save(saveContext)
    }
  }

  private class Node2VecModelReader extends MLReader[Node2VecModel] {
    private val className = classOf[Node2VecModel].getName

    private def createAngLoadPath(walkPathDir: String, name: String, maxNodeId: Long, psPartitionNum: Int): PSMatrix = {
      val mc: MatrixContext = new MatrixContext()
      mc.setName(name)
      mc.setRowType(RowType.T_ANY_LONGKEY_SPARSE)
      mc.setRowNum(1)
      mc.setColNum(maxNodeId)
      mc.setMaxColNumInBlock((maxNodeId + psPartitionNum - 1) / psPartitionNum)
      mc.setValueType(classOf[WalkPath])
      val walkPath = PSMatrix.matrix(mc)

      walkPath
    }

    override def load(path: String): Node2VecModel = {
      val metadata = Compat.defaultParamsReader.loadMetadata(path, sc, className)
      val model = new Node2VecModel(metadata.uid)
      Compat.defaultParamsReader.getAndSetParams(model, metadata)

      val dataPath = new Path(path, "data").toString
      val data = sparkSession.read.format("parquet").load(dataPath)

      val Row(name: String, maxNodeId: Long, psPartitionNum: Int) = data
        .select("name", "maxNodeId", "psPartitionNum").head()

      val walkPathDir = new Path(path, "walk").toString
      val walkPath = createAngLoadPath(walkPathDir, name, maxNodeId, psPartitionNum)
      model.walkPath = walkPath
      val loadContext = new ModelLoadContext(walkPathDir)
      loadContext.addMatrix(new MatrixLoadContext(name))
      val psContext = PSContext.getOrCreate(SparkContext.getOrCreate())
      psContext.load(loadContext)
      psContext.refreshMatrix()

      model
    }
  }

  override def read: MLReader[Node2VecModel] = new Node2VecModelReader()
}
