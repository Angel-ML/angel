package com.tencent.angel.spark.automl.feature

import com.tencent.angel.spark.automl.feature.TransformerWrapper
import com.tencent.angel.spark.automl.feature.preprocess.{MinMaxScalerWrapper, StandardScalerWrapper}
import org.apache.spark.ml.PipelineModel
import org.apache.spark.ml.linalg.Vectors
import org.apache.spark.sql.SparkSession

object PipelineDriver {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder().master("local").getOrCreate()

    //    val inputDF = spark.createDataFrame(Seq(
    //      (0L, "a b c d e spark", 1.0),
    //      (1L, "b d", 0.0),
    //      (2L, "spark f g h", 1.0),
    //      (3L, "hadoop mapreduce", 0.0)
    //    )).toDF("id", "text", "label")

    val inputDF = spark.createDataFrame(Seq(
      (0, Vectors.dense(1.0, 0.1, -1.0)),
      (1, Vectors.dense(2.0, 1.1, 1.0)),
      (2, Vectors.dense(3.0, 10.1, 3.0))
    )).toDF("id", "numerical")

    val pipelineWrapper = new PipelineWrapper()

    val transformers = Array[TransformerWrapper](
      new MinMaxScalerWrapper(),
      new StandardScalerWrapper()
    )

    val stages = PipelineBuilder.build(transformers)

    print(transformers(0).getInputCols)

    pipelineWrapper.setStages(stages)

    val model: PipelineModelWrapper = pipelineWrapper.fit(inputDF)

    val outDF = model.transform(inputDF)

    outDF.show()

  }

}
