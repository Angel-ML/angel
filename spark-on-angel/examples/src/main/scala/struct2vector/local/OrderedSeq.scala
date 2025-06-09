package struct2vector.local
import scala.io.Source
import Array._
import scala.collection.mutable
import scala.collection.mutable.Map
import scala.collection.mutable.ArrayBuffer

object OrderedSeq {
  var graph: Map[Int,ArrayBuffer[Int]] = Map()
  var degree = Array.fill[Int](50)(0)
  var comp_ordered_degree_seq: ArrayBuffer[ArrayBuffer[Array[Int]]] = new ArrayBuffer[ArrayBuffer[Array[Int]]]

  def getInput(): Unit = {
    val input = Source.fromFile("data/bc/karate_club_network.txt").getLines()
    for (i <- input) {
      val sd = i.split(' ')
//      print(sd(0).toInt, sd(1).toInt)
      if(graph.contains(sd(0).toInt)) {
        graph(sd(0).toInt) += sd(1).toInt
      }
      else {
        val tmp = ArrayBuffer(sd(1).toInt)
        graph += (sd(0).toInt->tmp)
      }
      degree(sd(0).toInt) = degree(sd(0).toInt) + 1
      degree(sd(1).toInt) = degree(sd(1).toInt) + 1 //度包括入度和出度
    }
  }

  def getOrderedDegreeSeq (node: Int, max_layers: Int): ArrayBuffer[Array[Int]] = {
    var ordered_degree_seq = new ArrayBuffer[Array[Int]]
    var q = new mutable.Queue[Int]
    var cur_layer = 0
    q += node
    q += -1 //标记一层结束
    var visited = Array.fill[Boolean](50)(false)
    visited(node) = true

    while (q.nonEmpty && cur_layer < max_layers) {
      var degree_seq = new ArrayBuffer[Int]
      var cur_node = q.dequeue()
      while (cur_node != -1) {
        degree_seq += degree(cur_node)
        visited(cur_node) = true
        if (graph.contains(cur_node)) {
          for (next_node <- graph(cur_node)) {
            if (!visited(next_node)) {
              q += next_node
            }
          }
        }
        q += -1
        var _degree_seq: Array[Int] = degree_seq.sorted.toArray //升序
        ordered_degree_seq += _degree_seq
//        print(degree_seq)
//        print("ordered1:"+ordered_degree_seq)
        cur_node = q.dequeue()
      }
    }
//    print("\n")
//    print("ordered:"+ordered_degree_seq)
//    print("\n")
    return ordered_degree_seq
  }

  def main(args: Array[String]): Unit = {
    getInput()
//    print(graph)
//    for (i<-degree) {
//      print(i)
//    }
    for (i <- 0 to 49) {
      if (degree(i) != 0) {
        comp_ordered_degree_seq += getOrderedDegreeSeq(i,50)
      }
    }
    var layer = 0
    print("Ordered Degree Sequence('\\' separates degree sequence for different nodes):\n")
    for (ord_deg_seq <- comp_ordered_degree_seq) {
      print("layer" + layer + " - ")
      layer = layer + 1
      for (deg_list <- ord_deg_seq) {
        for (i <- deg_list) {
          print(i+" ")
        }
        print("/")
      }
      print("\n")
    }

  }
}