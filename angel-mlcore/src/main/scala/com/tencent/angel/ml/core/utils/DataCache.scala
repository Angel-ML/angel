package com.tencent.angel.ml.core.utils

import java.util
import java.util.concurrent.locks.ReentrantReadWriteLock

import com.tencent.angel.ml.servingmath2.vector.Vector
import com.tencent.angel.ml.servingmath2.matrix.Matrix

class DataCache() {
  private val matCache = new util.HashMap[String, Matrix]()
  private val vecCache = new util.HashMap[String, Vector]()

  private val lock = new ReentrantReadWriteLock()
  private val readLock = lock.readLock()
  private val writeLock = lock.writeLock()

  def addMatrix(name:String, mat: Matrix): Unit = {
    writeLock.lock()
    try {
      matCache.put(name, mat)
    } finally {
      writeLock.unlock()
    }
  }

  def addVector(name:String, vec: Vector): Unit = {
    writeLock.lock()
    try {
      vecCache.put(name, vec)
    } finally {
      writeLock.unlock()
    }
  }

  def hasMatrix(name:String): Boolean = {
    readLock.lock()

    try {
      matCache.containsKey(name)
    } finally {
      readLock.unlock()
    }
  }

  def hasVector(name:String): Boolean = {
    readLock.lock()

    try {
      vecCache.containsKey(name)
    } finally {
      readLock.unlock()
    }
  }

  def getMatrix(name: String): Matrix = {
    readLock.lock()

    try{
      matCache.getOrDefault(name, null.asInstanceOf[Matrix])
    } finally {
      readLock.unlock()
    }
  }

  def getVector(name: String): Vector = {
    readLock.lock()

    try{
      vecCache.getOrDefault(name, null.asInstanceOf[Vector])
    } finally {
      readLock.unlock()
    }
  }

  def clearAll(): Unit = {
    writeLock.lock()
    try {
      matCache.clear()
      vecCache.clear()
    } finally {
      writeLock.unlock()
    }
  }

  def clearMatrix(): Unit = {
    writeLock.lock()
    try {
      matCache.clear()
    } finally {
      writeLock.unlock()
    }
  }

  def clearVector(): Unit = {
    writeLock.lock()
    try {
      vecCache.clear()
    } finally {
      writeLock.unlock()
    }
  }
}
