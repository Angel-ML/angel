package com.tencent.angel.spark.ml.fpm.Apriori


import scala.collection.immutable.HashMap
import scala.collection.mutable.ArrayBuffer

class CandidateTrie {

  var root = new TrieNode

  // insert a candidate pattern to trie
  def insert(pattern: Array[String]): Unit = {
    if (pattern == null || pattern.isEmpty) {
      return
    }

    root.insert(pattern, 0)
  }


  def search(transaction: Array[String], delimiter: String): Array[String] = {
    if (transaction == null || transaction.isEmpty) {
      return Array("")
    }
    return root.searchOccurrence(transaction, 0, delimiter)
  }

  // search for occurrences of a candidate pattern from trie
  def has(pattern: Array[String]): Boolean = {
    if (pattern == null || pattern.isEmpty) {
      return false
    }
    return root.has(pattern, 0)
  }
}

object CandidateTrie {
  def main(args: Array[String]): Unit = {
    val s1 = Array("a", "b", "c")
    val s2 = Array("b", "c", "d")
    val s3 = Array("a", "b", "d")
    val s5 = Array("a", "b", "e")
    val s4 = Array("a", "b", "c", "d", "e", "f")

    val trie = new CandidateTrie
    trie.insert(s1)
    trie.insert(s2)
    trie.insert(s3)
    trie.insert(s5)

    // scalastyle:off println
    System.out.println(trie.search(s4, ",").mkString)
    // scalastyle:on println
  }
}

class TrieNode {

  var map = new HashMap[String, TrieNode]


  // recursively insert pattern to trie

  def insert(pattern: Array[String], index: Int): Unit = {
    if (index >= pattern.length) {
      return
    }

    if (!map.contains(pattern(index))) {
      map.+=((pattern(index), new TrieNode))
    }

    map.get(pattern(index)).get.insert(pattern, index + 1)
  }

  // recursively serarch for an occurrence
  def has(pattern: Array[String], index: Int): Boolean = {

    if (index >= pattern.length) {
      return false
    }

    val item = pattern(index)

    if (!this.map.contains(item)) {
      return false
    } else {
      val child = this.map.get(item).get
      if (child.map.isEmpty && index == pattern.length - 1) {
        return true
      } else {
        return child.has(pattern, index + 1)
      }
    }
  }

  // search candidates from a transaction
  def searchOccurrence(transaction: Array[String], k: Int, delimiter: String): Array[String] = {
    if (transaction == null || transaction.length < k) {
      return Array("")
    }
    // val count = new ArrayBuffer[(String,Int)]
    val patterns = new ArrayBuffer[String]()
    search(transaction, 0, "", patterns, delimiter)
    patterns.toArray
  }

  def search(
              transaction: Array[String],
              index: Int,
              pattern: String,
              patterns: ArrayBuffer[String],
              delimiter: String): Unit = {

    if (index >= transaction.length) {
      return
    }
    val item = transaction(index)
    search(transaction, index + 1, pattern, patterns, delimiter)
    if (map.contains(item)) {
      val child = map.get(item).get
      var newPattern = ""

      if (pattern.isEmpty) {
        newPattern = pattern + item
      } else {
        newPattern = pattern + delimiter + item
      }
      if (child.map.isEmpty) {
        // hit bottom
        patterns.+=(newPattern)
      } else {
        child.search(transaction, index + 1, newPattern, patterns, delimiter)
      }
    }
    patterns.toArray
  }
}
