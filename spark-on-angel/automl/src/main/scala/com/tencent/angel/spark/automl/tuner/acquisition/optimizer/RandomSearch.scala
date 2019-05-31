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


package com.tencent.angel.spark.automl.tuner.acquisition.optimizer
import org.apache.spark.ml.linalg.Vectors
import com.tencent.angel.spark.automl.tuner.TunerParam
import com.tencent.angel.spark.automl.tuner.acquisition.Acquisition
import com.tencent.angel.spark.automl.tuner.config.{Configuration, ConfigurationSpace}
import org.apache.commons.logging.{Log, LogFactory}

import scala.collection.mutable.ArrayBuffer
import scala.util.Random

/**
  * Get candidate solutions via random sampling of configurations.
  *
  * @param acqFunc     : The acquisition function which will be maximized
  * @param configSpace : Configuration space of parameters
  * @param seed
  */
class RandomSearch(
                    override val acqFunc: Acquisition,
                    override val configSpace: ConfigurationSpace,
                    seed: Int = 100) extends AcqOptimizer(acqFunc, configSpace) {

  val LOG: Log = LogFactory.getLog(classOf[RandomSearch])

  val rd = new Random(seed)

  override def maximize(numPoints: Int, sorted: Boolean = true): (Array[(Double, Configuration)],Array[Array[Double]]) = {
    //println(s"maximize RandomSearch")
    val (configs: Array[Configuration],cateValues:Array[Array[Double]])  = configSpace.sample(TunerParam.sampleSize)
    if (configs.isEmpty) {
      (Array[(Double, Configuration)](),Array[Array[Double]]())
    } else {
      //configs.foreach { config =>
      //  println(s"sample a configuration: ${config.getVector.toArray.mkString(",")}")
      //}
      val retConfigs = if (sorted) {
        configs.map { config =>
          val configArray = config.getVector.toArray
          var oneHotConfig = new ArrayBuffer[Double]()
          for ( (cate, config) <- (cateValues zip configArray)){
            if (cate.contains(Double.MaxValue)){
              oneHotConfig +=config
            }
            else {
              cate.foreach{ value =>
                if(value == config){
                  oneHotConfig += 1.0
                }
                else {
                  oneHotConfig += 0.0
                }
              }
            }
          }
          (acqFunc.compute(Vectors.dense(oneHotConfig.toArray))._1, config)
        }.sortWith(_._1 > _._1).take(numPoints)
      }
      else {
        rd.shuffle(configs.map { config =>
          val configArray = config.getVector.toArray
          var oneHotConfig = new ArrayBuffer[Double]()
          for ( (cate, config) <- (cateValues zip configArray)){
            if (cate.contains(Double.MaxValue)){
              oneHotConfig +=config
            }
            else {
              cate.foreach{ value =>
                if(value == config){
                  oneHotConfig += 1.0
                }
                else {
                  oneHotConfig += 0.0
                }
              }
            }
          }
          (acqFunc.compute(Vectors.dense(oneHotConfig.toArray))._1, config)
        }.toTraversable).take(numPoints).toArray
      }
      (retConfigs,cateValues)
    }
  }

  override def maximize: (Array[(Double, Configuration)],Array[Array[Double]]) = {
    maximize(1, true)
  }
}
