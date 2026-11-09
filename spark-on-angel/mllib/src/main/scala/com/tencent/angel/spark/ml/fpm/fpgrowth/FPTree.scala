package com.tencent.angel.spark.ml.fpm.fpgrowth

import com.tencent.angel.spark.ml.fpm.fpgrowth.FPTree._

import scala.collection.mutable
import scala.collection.mutable.ListBuffer

private[fpm] class FPTree extends Serializable {
  val root: FPNode = new FPNode(null)
  private val records: mutable.HashMap[Int, Record] = mutable.HashMap.empty
  
  /** add items to FPTree
    *
    * @param items items
    * @param count count of this transaction
    * @return FPTree
    */
  def addToFPTree(items: Iterable[Int], count: Long = 1L): FPTree = {
    assert(count > 0, "require count >= 1")
    var curr = root
    curr.count += count
    items.foreach{ item =>
      val rec = this.records.getOrElseUpdate(item, new Record)
      rec.count += count
      
      val child = curr.children.getOrElseUpdate(item, {
        val newChild = new FPNode(curr)
        newChild.itemId = item
        rec.record += newChild
        newChild
      })
      
      child.count += count
      curr = child
    }
    this
  }
  
  def merge(other: FPTree): FPTree = {
    other.getTransactions().foreach {
      case (trans, count) =>
        addToFPTree(trans, count)
    }
    this
  }
  
  /** using suffix to build a new FPTree.
    * for recursive search purpose
    *
    * @param suffix suffix of association chain
    * @return new FPTree with items associated with suffix
    */
  private def buildTreeWithSuffix(suffix: Int): FPTree = {
    val suffTree = new FPTree
    
    if (this.records.contains(suffix)) {
      val rec = this.records(suffix)
      rec.record.foreach{ r =>
        var curr = r.parent
        val tList: ListBuffer[Int] = ListBuffer.empty
        while (!curr.isRootNode()) {
          tList.insert(0, curr.itemId)
          curr = curr.parent
        }
        suffTree.addToFPTree(tList.toList, r.count)
      }
    }
    suffTree
  }
  
  /** get all transactions under root
    *
    * @return
    */
  def getTransactions(): Array[(List[Int], Long)] = {
    getTransactions(this.root)
  }
  
  
  /** get transactions under node
    *
    * @param node under which node the transactions are generated
    * @return transactions and there counts
    */
  private def getTransactions(node: FPNode): Array[(List[Int], Long)] = {
    var count = node.count
    val trans = node.children.iterator.flatMap { case (item, child) =>
      getTransactions(child).map { case (t, c) =>
        count -= c
        (item :: t, c)
      }
    }.toArray

    trans ++ {
      if (count > 0) {
        Iterator.single((Nil, count))
      } else {
        Iterator.empty
      }
    }
  }
  
  /** get all the patterns that with frequency over min count
    *
    * @param minCount the min count limit of frequency
    * @param suffixInPart if the suffix is in the partition it should be done
    * @return (patterns, count)
    */
  private[fpm] def getPatterns(minCount: Long, suffixInPart: Int => Boolean = _ => true): Iterator[(List[Int], Long)] = {
    records.iterator.flatMap{
      case (item, rec) =>
        if (suffixInPart(item) && rec.count >= minCount) {
          Iterator.single((item :: Nil, rec.count)) ++
            buildTreeWithSuffix(item).getPatterns(minCount).map{
              case (pt, cnt) =>
                (item :: pt, cnt)
            }
        }
        else {
          Iterator.empty
        }
    }
  }
  
}
private[fpm] object FPTree {
  class FPNode(val parent: FPNode) extends Serializable {
    var itemId: Int = _
    val children: mutable.HashMap[Int, FPNode] = mutable.HashMap.empty
    var count: Long = 0L
    
    def isRootNode(): Boolean = {
      parent == null
    }
    
    def isLeafNode(): Boolean = {
      this.children.isEmpty
    }
  }
  
  private class Record extends Serializable {
    var count: Long = 0L
    val record: ListBuffer[FPNode] = ListBuffer.empty
  }
}