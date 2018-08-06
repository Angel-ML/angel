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

package com.tencent.angel.ml.matrixfactorization.threads

import java.io.{BufferedReader, File, FileReader, IOException}

import com.tencent.angel.ml.math.vector.DenseFloatVector
import com.tencent.angel.ml.matrixfactorization.MFModel
import it.unimi.dsi.fastutil.ints.IntArrayList
import org.apache.commons.logging.{Log, LogFactory}

import scala.collection.Map
import scala.collection.mutable.HashMap

object Utils {
  private val LOG: Log = LogFactory.getLog(Utils.getClass)

  @throws[IOException]
  def readNetflix(path: String): Map[Int, UserVec] = {

    val builder1 = new HashMap[Int, IntArrayList]
    val builder2 = new HashMap[Int, IntArrayList]

    val reader = new BufferedReader(new FileReader(new File(path)))
    var line = new String

    while ( {
      line = reader.readLine();
      line != null
    }) {
      val parts = line.split(",")
      val itemId = parts(0).toInt
      val userId = parts(1).toInt
      val rating = parts(2).toInt

      if (!builder1.keySet.contains(userId)) {
        builder1.put(userId, new IntArrayList)
        builder2.put(userId, new IntArrayList)
      }

      builder1(userId).add(itemId)
      builder2(userId).add(rating)
    }

    val users = new HashMap[Int, UserVec]

    for (userId <- builder1.keySet) {
      val itemIds = builder1(userId)
      val ratings = builder2(userId)
      users.put(userId, new UserVec(userId, itemIds.toIntArray, ratings.toIntArray))
    }

    return users
  }

  @throws[IOException]
  def read(path: String): Map[Int, UserVec] = {

    val users = new HashMap[Int, UserVec]

    val reader = new BufferedReader(new FileReader(new File(path)))
    var line = new String

    while ( {
      line = reader.readLine;
      line != null
    }) {
      val parts = line.split(" ")
      val userId = parts(0).toInt
      val numRatings = (parts.length - 1) / 2
      val itemIds = new Array[Int](numRatings)
      val ratings = new Array[Int](numRatings)

      for (i <- 0 until numRatings) {
        itemIds(i) = parts(i * 2 + 1).toInt
        ratings(i) = parts(i * 2 + 2).toInt
      }

      val user = new UserVec(userId, itemIds, ratings)
      users.put(userId, user)
    }

    return users
  }

  @throws[IOException]
  def readStradsMeta(path: String): Map[String, Int] = {
    val info = new HashMap[String, Int]

    val reader = new BufferedReader(new FileReader(new File(path)))
    reader.readLine
    val meta = reader.readLine
    val parts = meta.split(" ")
    val row = parts(0).toInt
    val col = parts(1).toInt
    val nnz = parts(2).toInt
    info.put("row", row)
    info.put("col", col)
    info.put("nnz", nnz)

    return info
  }

  @throws[IOException]
  def readStrads(path: String): Map[Int, UserVec] = {
    val users = new HashMap[Int, UserVec]
    val builder1 = new HashMap[Int, IntArrayList]
    val builder2 = new HashMap[Int, IntArrayList]

    val reader = new BufferedReader(new FileReader(new File(path)))
    reader.readLine
    val meta = reader.readLine
    var parts = meta.split(" ")
    val row = parts(0).toInt
    val col = parts(1).toInt
    val nnz = parts(2).toInt

    var line = new String
    while ( {
      line = reader.readLine;
      line != null
    }) {
      parts = line.split(" ")
      val userId = parts(0).toInt
      val itemId = parts(1).toInt
      val rating = parts(2).toDouble.toInt
      if (!builder1.keySet.contains(userId)) {
        builder1.put(userId, new IntArrayList)
        builder2.put(userId, new IntArrayList)
      }
      builder1(userId).add(itemId)
      builder2(userId).add(rating)
    }

    for (userId <- builder1.keySet) {
      val itemIds = builder1(userId)
      val ratings = builder2(userId)
      val userVec = new UserVec(userId, itemIds.toIntArray, ratings.toIntArray)
      users.put(userId, userVec)
    }

    reader.close

    return users
  }

  def buildItems(mFModel: MFModel) = {
    val users = mFModel.users

    val items = new HashMap[Int, ItemVec]
    val builderForUsers = new HashMap[Int, IntArrayList]
    val builderForRating = new HashMap[Int, IntArrayList]

    // Traverse users, building items dynamically
    for (user <- users.values) {
      val itemIds = user.getItemIds
      val ratings = user.getRatings
      val length = itemIds.length

      for (i <- 0 until length) {
        val itemId = itemIds(i)
        val rating = ratings(i)

        if (!builderForUsers.keySet.contains(itemId)) {
          builderForUsers.put(itemId, new IntArrayList)
          builderForRating.put(itemId, new IntArrayList)
        }
        builderForUsers(itemId).add(user.getUserId)
        builderForRating(itemId).add(rating)
      }
    }

    // Constructing items to ItemVec
    for (itemId <- builderForUsers.keySet) {
      val userIdArr = builderForUsers(itemId)
      val ratingArr = builderForRating(itemId)
      val item = new ItemVec(itemId, userIdArr.toIntArray, ratingArr.toIntArray)
      items.put(itemId, item)
    }

    mFModel.buildItems(items)

    // Cache the Ids of used items
    for (itemId <- items.keySet) {
      mFModel.setUsedItem(itemId)
    }
  }

  def sgdOneRating(Li: DenseFloatVector, Rj: DenseFloatVector, rating: Int, eta: Double,
                   lambda: Double) {
    val r = rating.toDouble
    val t = Li.dot(Rj)

    if (t.isNaN) {
      println("t is NaN")

      for (i <- 0 until Li.size()) {
        System.out.print(Li.get(i) + ", ")
      }
      println()

      for (i <- 0 until Rj.size()) {
        System.out.print(Rj.get(i) + ", ")
      }
      println()

      System.exit(0)
    }

    val u = r - Li.dot(Rj)

    if (u.isNaN) {
      println("u is NaN")
      System.exit(0)
    }
    else if (u > 10000000) {
      println(u)
    }

    // update Li
    Li.timesBy(1 - lambda * eta).plusBy(Rj, eta * u)

    // update local Rj
    Rj.timesBy(1 - lambda * eta).plusBy(Li, eta * u)

  }

  def sgdOneItemVec(itemVec: ItemVec, Rj: DenseFloatVector, L: Array[DenseFloatVector], eta:
  Double, lambda: Double) {
    val userIds = itemVec.getUserIds
    val ratings = itemVec.getRatings
    val length = ratings.length

    for (i <- 0 until length) {
      val userId = userIds(i)
      val rating = ratings(i)
      val Li = L(userId)
      Li synchronized {
        sgdOneRating(Li, Rj, rating, eta, lambda)
      }
    }

  }

  def sgdOneRating(Li: DenseFloatVector, Rj: DenseFloatVector, rating: Int, eta: Double, lambda:
  Double, update: DenseFloatVector) {
    val r = rating.toDouble
    val u = r - Li.dot(Rj)

    // update Li
    Li.timesBy(1 - lambda * eta).plusBy(Rj, 2 * eta * u)

    // accumulate update for Rj
    update.plusBy(Rj, -1 * lambda * eta).plusBy(Li, 2 * eta * u)

    // update local Rj
    Rj.timesBy(1 - lambda * eta).plusBy(Li, 2 * eta * u)
  }

  def sgdOneItemVec(itemVec: ItemVec, Rj: DenseFloatVector, users: Map[Int, UserVec],
                    eta: Double, lambda: Double, rank: Int): DenseFloatVector = {
    val userIds = itemVec.getUserIds
    val ratings = itemVec.getRatings
    val update = new DenseFloatVector(rank)
    val length = ratings.length

    for (i <- 0 until length) {
      val userId = userIds(i)
      val rating = ratings(i)
      val Li = users(userId).getFeatures
      Li synchronized {
        sgdOneRating(Li, Rj, rating, eta, lambda, update)
      }
    }

    return update
  }

  def lossOneRating(Li: DenseFloatVector, Rj: DenseFloatVector, rating: Int, eta: Double, lambda:
  Double, rank: Int): Double = {
    val r = rating.toDouble
    val u = r - Li.dot(Rj)
    var e = 0.0
    e += Math.pow(u, 2.0)
    //    LOG.info("e="+e);
    //    e += (lambda / 2) * (Li.square() + Rj.square());
    //    e += (lambda / 2) * (Li.square());
    return e
  }

  def lossOneRow(LiOrRj: DenseFloatVector, lambda: Double): Double = {
    return (lambda / 2) * LiOrRj.squaredNorm()
  }

  def lossOneItemVec(itemVec: ItemVec, Rj: DenseFloatVector, L: Array[DenseFloatVector], eta: Double,
                     lambda: Double, rank: Int): Double = {
    var loss = 0.0
    val userIds = itemVec.getUserIds
    val ratings = itemVec.getRatings
    val length = ratings.length

    for (i <- 0 until length) {
      val userId = userIds(i)
      val rating = ratings(i)
      val Li = L(userId)

      loss += lossOneRating(Li, Rj, rating, eta, lambda, rank)
    }

    return loss
  }

  def lossOneItemVec(itemVec: ItemVec, Rj: DenseFloatVector, users: Map[Int, UserVec],
                     eta: Double, lambda: Double, rank: Int): Double = {
    var loss = 0.0
    val userIds = itemVec.getUserIds
    val ratings = itemVec.getRatings
    val length = ratings.length

    for (i <- 0 until length) {
      val userId = userIds(i)
      val rating = ratings(i)
      val Li = users(userId).getFeatures
      loss += lossOneRating(Li, Rj, rating, eta, lambda, rank)
    }
    return loss
  }
}