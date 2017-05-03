package com.tencent.angel.spark.examples

import breeze.linalg.DenseVector
import breeze.math.MutableEnumeratedCoordinateField

import com.tencent.angel.spark.PSClient
import com.tencent.angel.spark.examples.pof._
import com.tencent.angel.spark.examples.util.PSExamples._

/**
 * These are the examples of PS Oriented Functions(POF) in machine learning cases.
 */
object AngelMapFunction {

  def main(args: Array[String]): Unit = {
    parseArgs(args)
    runWithSparkContext(this.getClass.getSimpleName) { sc =>
      PSClient.setup(sc)
      run()
      runWithPS()
      runWithPSBreeze()
    }
  }

  def run()(implicit space: MutableEnumeratedCoordinateField[DenseVector[Double], Int, Double])
  : Unit = {
    val a = DenseVector.fill(DIM, 1.0)
    val b = DenseVector.fill(DIM, 2.0)
    val c = a.map(_ * 4.2)
    val d = space.zipMapValues.map(a, b, (x, y) => x / y)
    val e = a.mapPairs((index, value) => if (index == 2) 0 else value)
    val f = space.zipMapKeyValues.map(a, b, (index, x, y) => if (index == 2) 0 else x + y)
    println("c: " + c)
    println("d: " + d)
    println("e: " + e)
    println("f: " + f)
  }

  def runWithPS(): Unit = {
    val client = PSClient.get
    val pool = client.createVectorPool(DIM, 10)
    val a = pool.create(1.0)
    val b = pool.create(2.0)
    val c = pool.allocate()
    val d = pool.allocate()
    client.map(a, new MulScalar(4.2), c)
    client.zip2Map(a, b, new ZipDiv, d)
    val e = pool.allocate()
    val f = pool.allocate()
    client.mapWithIndex(a, new Filter(2), e)
    client.zip2MapWithIndex(a, b, new FilterZipAdd(2), f)
    println("c: " + client.get(c).mkString("Array(", ", ", ")"))
    println("d: " + client.get(d).mkString("Array(", ", ", ")"))
    println("e: " + client.get(e).mkString("Array(", ", ", ")"))
    println("f: " + client.get(f).mkString("Array(", ", ", ")"))
    val g = pool.allocate()
    client.zip3Map(b, c, d, new Zip3Add, g)
    println("g: " + client.get(g).mkString("Array(", ", ", ")"))
    client.map(a, new MulScalar(2), a)
    println("new a: " + client.get(a).mkString("Array(", ", ", ")"))
    client.destroyVectorPool(pool)
  }

  def runWithPSBreeze(): Unit = {
    val client = PSClient.get
    val pool = client.createVectorPool(DIM, 10)
    val a = pool.create(1.0).mkBreeze()
    val b = pool.create(2.0).mkBreeze()
    val c = a.map(new MulScalar(4.2))
    val d = a.zipMap(b, new ZipDiv)
    val e = a.mapWithIndex(new Filter(2))
    val f = a.zipMapWithIndex(b, new FilterZipAdd(2))
    println("c: " + client.get(c.proxy).mkString("Array(", ", ", ")"))
    println("d: " + client.get(d.proxy).mkString("Array(", ", ", ")"))
    println("e: " + client.get(e.proxy).mkString("Array(", ", ", ")"))
    println("f: " + client.get(f.proxy).mkString("Array(", ", ", ")"))
    val g = b.zipMap(c, d, new Zip3Add)
    println("g: " + client.get(g.proxy).mkString("Array(", ", ", ")"))
    a.mapInto(new MulScalar(2))
    println("new a: " + client.get(a.proxy).mkString("Array(", ", ", ")"))
    client.destroyVectorPool(pool)
  }

}
