package com.tencent.angel.spark.vector

import com.tencent.angel.spark.{PSClient, PSFunSuite, PSVectorPool, SharedPSContext}
import org.apache.spark.rdd.RDD
import org.scalatest.Matchers

import scala.util.Random

class RemotePSVectorSuite extends PSFunSuite with Matchers with SharedPSContext with Serializable {

  private val dim = 10
  private val capacity = 10
  private val rddCount = 11
  private val partitionNum = 3
  private var _psClient: PSClient = _
  private var _pool: PSVectorPool = _

  @transient private var _rdd: RDD[Array[Double]] = _
  @transient private var _localSum: Array[Double] = _
  @transient private var _localMax: Array[Double] = _
  @transient private var _localMin: Array[Double] = _

  override def beforeAll(): Unit = {
    super.beforeAll()
    _psClient = PSClient.get
    _pool = _psClient.createVectorPool(dim, capacity)

    val thisDim = dim
    _rdd = sc.parallelize(0 until rddCount, partitionNum).map {i =>
      val rand = new Random(42)
      (0 until thisDim).toArray.map(i => rand.nextGaussian())
    }
    _rdd.cache()
    _rdd.count()

    _localSum = new Array[Double](dim)
    _localMax = new Array[Double](dim)
    _localMin = new Array[Double](dim)
    _rdd.collect().foreach { arr =>
      arr.indices.foreach { i =>
        _localSum(i) += arr(i)
        if (_localMax(i) < arr(i)) _localMax(i) = arr(i)
        if (_localMin(i) > arr(i)) _localMin(i) = arr(i)
      }
    }
  }

  override def afterAll(): Unit = {
    _rdd.unpersist()
    _rdd = null
    _psClient.destroyVectorPool(_pool)
    _pool = null
    _psClient = null
    super.afterAll()
  }


  test("increment dense vector") {
    val remoteVector = _pool.createZero().mkRemote()
    val rdd2 = _rdd.mapPartitions { iter =>
      iter.foreach { arr =>
        remoteVector.increment(arr)
      }
      remoteVector.flush()
      Iterator.empty
    }
    rdd2.count()

    val psArray = remoteVector.toLocal.get()
    _localSum.indices.foreach { i => assert(_localSum(i) === psArray(i) +- doubleEps) }
  }

  test("incrementAndFlush") {
    val remoteVector = _pool.createZero().mkRemote()
    val rdd2 = _rdd.mapPartitions { iter =>
      val sum = iter.reduce { (arr1: Array[Double], arr2: Array[Double]) =>
        arr1.indices.toArray.map (i => arr1(i) + arr2(i))
      }
      remoteVector.incrementAndFlush(sum)
      Iterator.empty
    }
    rdd2.count()

    val psArray = remoteVector.toLocal.get()
    _localSum.indices.foreach { i => assert(_localSum(i) === psArray(i) +- doubleEps) }
  }

  test("mergeMax dense Vector") {
    val remoteVector = _pool.createZero().mkRemote()

    val rdd2 = _rdd.mapPartitions { iter =>
      iter.foreach( arr => remoteVector.mergeMax(arr) )
      remoteVector.flush()
      Iterator.empty
    }
    rdd2.count()

    assert(remoteVector.toLocal.get().sameElements(_localMax))
  }


  test("mergeMaxAndFlush") {
    val remoteVector = _pool.createZero().mkRemote()

    val thisDim = dim
    val rdd2 = _rdd.mapPartitions { iter =>
      val max = Array.ofDim[Double](thisDim)
      iter.foreach { arr =>
        arr.indices.foreach { i => if (arr(i) >  max(i)) max(i) = arr(i) }
      }
      remoteVector.mergeMaxAndFlush(max)
      Iterator.empty
    }
    rdd2.count()

    assert(remoteVector.toLocal.get().sameElements(_localMax))
  }

  test("mergeMin dense vector") {
    val remoteVector = _pool.createZero().mkRemote()

    val rdd2 = _rdd.mapPartitions { iter =>
      iter.foreach( arr => remoteVector.mergeMin(arr) )
      remoteVector.flush()
      Iterator.empty
    }
    rdd2.count()

    assert(remoteVector.toLocal.get().sameElements(_localMin))
  }

  test("mergeMinAndFlush") {
    val remoteVector = _pool.createZero().mkRemote()

    val thisDim = dim
    val rdd2 = _rdd.mapPartitions { iter =>
      val min = Array.ofDim[Double](thisDim)
      iter.foreach { arr =>
        arr.indices.foreach { i => if (arr(i) <  min(i)) min(i) = arr(i) }
      }
      remoteVector.mergeMinAndFlush(min)
      Iterator.empty
    }
    rdd2.count()

    assert(remoteVector.toLocal.get().sameElements(_localMin))
  }
}
