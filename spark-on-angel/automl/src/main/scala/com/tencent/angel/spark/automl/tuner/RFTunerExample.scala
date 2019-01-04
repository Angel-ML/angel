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


package com.tencent.angel.spark.automl.tuner

import com.tencent.angel.spark.automl.tuner.acquisition.optimizer.{AcqOptimizer, RandomSearch}
import com.tencent.angel.spark.automl.tuner.acquisition.{Acquisition, EI}
import com.tencent.angel.spark.automl.tuner.config.ConfigurationSpace
import com.tencent.angel.spark.automl.tuner.parameter.{ContinuousSpace, DiscreteSpace, ParamSpace}
import com.tencent.angel.spark.automl.tuner.solver.{Solver, SolverWithTrail}
import com.tencent.angel.spark.automl.tuner.surrogate.{RFSurrogate, Surrogate}
import com.tencent.angel.spark.automl.tuner.trail.{TestTrail, Trail}
import org.apache.spark.ml.linalg.Vector

object RFTunerExample extends App {

  override def main(args: Array[String]): Unit = {
    val param1: ParamSpace[Double] = new ContinuousSpace("param1", 0, 10, 11)
    val param2: ParamSpace[Double] = new ContinuousSpace("param2", -5, 5, 11)
    val param3: ParamSpace[Double] = new DiscreteSpace[Double]("param3", Array(0.0, 1.0, 3.0, 5.0))
    val param4: ParamSpace[Double] = new DiscreteSpace[Double]("param4", Array(-5.0, -3.0, 0.0, 3.0, 5.0))
    val cs: ConfigurationSpace = new ConfigurationSpace("cs")
    cs.addParam(param1)
    cs.addParam(param2)
    cs.addParam(param3)
    cs.addParam(param4)
    TunerParam.setBatchSize(1)
    TunerParam.setSampleSize(100)
    val sur: Surrogate = new RFSurrogate(cs, true)
    val acq: Acquisition = new EI(sur, 0.1f)
    val opt: AcqOptimizer = new RandomSearch(acq, cs)
    val solver: Solver = new Solver(cs, sur, acq, opt)
    val trail: Trail = new TestTrail()
    val runner: SolverWithTrail = new SolverWithTrail(solver, trail)
    val result: (Vector, Double) = runner.run(10)
    sur.stop()
    println(s"Best configuration ${result._1.toArray.mkString(",")}, best performance: ${result._2}")
  }
}
