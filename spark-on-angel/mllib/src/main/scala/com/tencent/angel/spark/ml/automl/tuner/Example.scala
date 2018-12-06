package com.tencent.angel.spark.ml.automl.tuner

import com.tencent.angel.ml.auto.acquisition.optimizer.{AcqOptimizer, RandomSearch}
import com.tencent.angel.ml.auto.acquisition.{Acquisition, EI}
import com.tencent.angel.ml.auto.config.ConfigurationSpace
import com.tencent.angel.ml.auto.parameter.{ContinuousSpace, DiscreteSpace, ParamSpace}
import com.tencent.angel.ml.auto.setting.Setting
import com.tencent.angel.ml.auto.solver.{Solver, SolverWithTrail}
import com.tencent.angel.ml.auto.surrogate.{RFSurrogate, Surrogate}
import com.tencent.angel.ml.auto.trail.{TestTrail, Trail}
import com.tencent.angel.ml.math2.vector.IntFloatVector

object Example extends App {

  override def main(args: Array[String]): Unit = {
    val param1: ParamSpace[Float] = new ContinuousSpace("param1", 0, 10, 11)
    val param2: ParamSpace[Float] = new ContinuousSpace("param2", -5, 5, 11)
    val param3: ParamSpace[Float] = new DiscreteSpace[Float]("param3", List(0.0f, 1.0f, 3.0f, 5.0f))
    val param4: ParamSpace[Float] = new DiscreteSpace[Float]("param4", List(-5.0f, -3.0f, 0.0f, 3.0f, 5.0f))
    val cs: ConfigurationSpace = new ConfigurationSpace("cs")
    cs.addParam(param1)
    cs.addParam(param2)
    cs.addParam(param3)
    cs.addParam(param4)
    Setting.setBatchSize(1)
    Setting.setSampleSize(100)
    val sur: Surrogate = new RFSurrogate(cs.paramNum, true)
    val acq: Acquisition = new EI(sur, 0.1f)
    val opt: AcqOptimizer = new RandomSearch(acq, cs)
    val solver: Solver = new Solver(cs, sur, acq, opt)
    val trail: Trail = new TestTrail()
    val runner: SolverWithTrail = new SolverWithTrail(solver, trail)
    val result: (IntFloatVector, Float) = runner.run(100)
    sur.stop()
    println(s"Best configuration ${result._1.getStorage.getValues.mkString(",")}, best performance: ${result._2}")
  }
}
