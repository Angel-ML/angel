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

package com.tencent.angel.spark.automl

import com.tencent.angel.spark.automl.feature.preprocess.{HashingTFWrapper, IDFWrapper, TokenizerWrapper}
import com.tencent.angel.spark.automl.feature.{PipelineBuilder, PipelineWrapper, TransformerWrapper}
import org.apache.spark.sql.SparkSession
import org.junit._

class PipelineTest {

  val spark = SparkSession.builder().master("local").getOrCreate()

  @Test def testTfIdf(): Unit = {
    val sentenceData = spark.createDataFrame(Seq(
      (0.0, "Hi I heard about Spark"),
      (0.0, "I wish Java could use case classes"),
      (1.0, "Logistic regression models are neat")
    )).toDF("label", "sentence")

    val pipelineWrapper = new PipelineWrapper()

    val transformers = Array[TransformerWrapper](
      new TokenizerWrapper(),
      new HashingTFWrapper(20),
      new IDFWrapper()
    )

    val stages = PipelineBuilder.build(transformers)

    transformers.foreach{ transformer =>
      val inutCols = transformer.getInputCols
      val outputCols = transformer.getOutputCols
      inutCols.foreach(print)
      print("    ")
      outputCols.foreach(print)
      println()
    }

    pipelineWrapper.setStages(stages)

    val model = pipelineWrapper.fit(sentenceData)

    val outputDF = model.transform(sentenceData)
    outputDF.select("outIDF").show()
    outputDF.select("outIDF").foreach{ row =>
      println(row.get(0).getClass.getSimpleName)
      val arr = row.get(0)
      println(arr.toString)
    }
    outputDF.rdd.map(row => row.toString()).repartition(1)
      .saveAsTextFile("output/tfidf")
  }

  @Test def testWord2Vec(): Unit = {

  }

  @Test def testNumerical(): Unit = {

  }

}
