package com.tencent.angel.spark.ml

import org.scalatest.{BeforeAndAfterAll, FunSuite, Outcome}


trait PSFunSuite extends FunSuite with BeforeAndAfterAll {

  final protected override def withFixture(test: NoArgTest): Outcome = {
    val testName = test.text
    val suiteName = this.getClass.getName
    try {
      println(s"\n\n===== TEST OUTPUT FOR $suiteName: '$testName' ======\n")
      test()
    } finally {
      println(s"\n===== FINISHED $suiteName: '$testName' ======\n")
    }
  }

}
