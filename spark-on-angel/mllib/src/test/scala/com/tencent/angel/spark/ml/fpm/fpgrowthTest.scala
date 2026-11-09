package com.tencent.angel.spark.ml.fpm

import com.tencent.angel.spark.ml.fpm.fpgrowth.FPTree
import org.scalatest.funsuite.AnyFunSuite

class fpgrowthTest extends AnyFunSuite{
  test("merge") {
    val tree1 = new FPTree
    val tree2 = new FPTree
    
    val tree3 = new FPTree
    tree3.addToFPTree(Array(1, 2, 3, 4))
    tree2.addToFPTree(Array(1, 2, 4, 5))
    
    val tree4 = tree2.merge(tree3)
    
  }
}
