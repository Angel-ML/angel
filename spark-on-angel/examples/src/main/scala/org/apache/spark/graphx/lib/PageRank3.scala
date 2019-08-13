package org.apache.spark.graphx.lib


import org.apache.spark.rdd.RDD
import org.apache.spark.graphx._
import utils.io.{Conf, Log}

/**
  * PageRank algorithm implementation. There are two implementations of PageRank implemented.
  *
  * The first implementation uses the standalone `Graph` interface and runs PageRank
  * for a fixed number of iterations:
  * {{{
  * var PR = Array.fill(n)( 1.0 )
  * val oldPR = Array.fill(n)( 1.0 )
  * for( iter <- 0 until numIter ) {
  *   swap(oldPR, PR)
  *   for( i <- 0 until n ) {
  *     PR[i] = alpha + (1 - alpha) * inNbrs[i].map(j => oldPR[j] / outDeg[j]).sum
  *   }
  * }
  * }}}
  *
  * The second implementation uses the `Pregel` interface and runs PageRank until
  * convergence:
  *
  * {{{
  * var PR = Array.fill(n)( 1.0 )
  * val oldPR = Array.fill(n)( 0.0 )
  * while( max(abs(PR - oldPr)) > tol ) {
  *   swap(oldPR, PR)
  *   for( i <- 0 until n if abs(PR[i] - oldPR[i]) > tol ) {
  *     PR[i] = alpha + (1 - \alpha) * inNbrs[i].map(j => oldPR[j] / outDeg[j]).sum
  *   }
  * }
  * }}}
  *
  * `alpha` is the random reset probability (typically 0.15), `inNbrs[i]` is the set of
  * neighbors which link to `i` and `outDeg[j]` is the out degree of vertex `j`.
  *
  * @note This is not the "normalized" PageRank and as a consequence pages that have no
  *       inlinks will have a PageRank of alpha.
  */
object PageRank3 {

  /**
    * Run PageRank for a fixed number of iterations returning a graph
    * with vertex attributes containing the PageRank and edge
    * attributes the normalized edge weight.
    *
    * @param graph     the graph on which to compute PageRank
    * @param numIter   the number of iterations of PageRank to run
    * @param resetProb the random reset probability (alpha)
    * @return the graph containing with each vertex containing the PageRank and each edge
    *         containing the normalized weight.
    */

  def runStatic(graph: Graph[Float, Float], resetProb: Float, numIter:Int)
  : RDD[Array[String]] = {
    require(numIter > 0, s"Number of iterations must be greater than 0," +
      s" but got $numIter")
    require(resetProb >= 0 && resetProb <= 1, s"Random reset probability must belong" +
      s" to [0, 1], but got $resetProb")
    // Initialize the PageRank graph with each edge attribute having
    // weight 1/outDegree and each vertex with attribute 1.0.
    val checkpointInterval = Conf.getCheckpointInterval
    val storageLevel = Conf.getStorageLevel
    Log.withTimePrintln("Using the static implementation")
    Log.withTimePrintln(s"numIter=$numIter, resetProb=$resetProb, checkpointInterval=$checkpointInterval, storageLevel=$storageLevel")
    val outDeg = graph.aggregateMessages[Float]({ t => t.sendToSrc(t.attr) }, _ + _)

    def maper(e: EdgeTriplet[Float, Float]): Float = e.attr / e.srcAttr

    var rankGraph: Graph[Float, Float] = graph
      // Associate the degree with each vertex
      .outerJoinVertices(outDeg) { (_, _, deg) => deg.getOrElse(0.0f) }
      // Set the weight on the edges based on the degree
      .mapTriplets[Float](e => maper(e), TripletFields.All)
      // Set the vertex attributes to the initial pagerank values
      .mapVertices { case (_, _) => 1.0f }


    var iteration = 0
    var prevRankGraph: Graph[Float, Float] = null
    while (iteration < numIter) {
      Log.withTimePrintln(s"[PageRank]processing $iteration / $numIter]")
      rankGraph.persist(storageLevel)

      // Compute the outgoing rank contributions of each vertex, perform local preaggregation, and
      // do the final aggregation at the receiving vertices. Requires a shuffle for aggregation.
      val rankUpdates = rankGraph.aggregateMessages[Float](
        ctx => ctx.sendToDst(ctx.srcAttr * ctx.attr), _ + _, TripletFields.Src)

      // Apply the final rank updates to get the new ranks, using join to preserve ranks of vertices
      // that didn't receive a message. Requires a shuffle for broadcasting updated ranks to the
      // edge partitions.
      prevRankGraph = rankGraph

      rankGraph = rankGraph.outerJoinVertices(rankUpdates) {
        (_, _, msgSumOpt) => resetProb + (1.0f - resetProb) * msgSumOpt.getOrElse(0.0f)
      }.persist(storageLevel)

      if ((iteration + 1) % checkpointInterval == 0)
        rankGraph.checkpoint()
      rankGraph.edges.foreachPartition(_ => {}) // also materializes rankGraph.vertices
      Log.withTimePrintln(s"[PageRank]finished $iteration/ $numIter")

      prevRankGraph.vertices.unpersist(false)
      prevRankGraph.edges.unpersist(false)

      iteration += 1
    }

    // SPARK-18847 If the graph has sinks (vertices with no outgoing edges) correct the sum of ranks
    normalizeRankSum(rankGraph)
  }


  /**
    * Run a dynamic version of PageRank returning a graph with vertex attributes containing the
    * PageRank and edge attributes containing the normalized edge weight.
    *
    * @param graph     the graph on which to compute PageRank
    * @param tol       the tolerance allowed at convergence (smaller => more accurate).
    * @param resetProb the random reset probability (alpha)
    * @return the graph containing with each vertex containing the PageRank and each edge
    *         containing the normalized weight.
    */

  def runDynamic(graph: Graph[Int, Float], tol: Float, resetProb: Float = 0.15f, maxIter: Int = Int.MaxValue)
  : RDD[Array[String]] = {
    require(tol >= 0, s"Tolerance must be no less than 0, but got $tol")
    require(resetProb >= 0 && resetProb <= 1, s"Random reset probability must belong" +
      s" to [0, 1], but got $resetProb")

    val checkpointInterval = Conf.getCheckpointInterval
    val storageLevel = Conf.getStorageLevel
    Log.withTimePrintln("Using the dynamic implementation")
    Log.withTimePrintln(s"maxIter=$maxIter, resetProb=$resetProb, tol=$tol, checkpointInterval=$checkpointInterval, storageLevel=$storageLevel")

    // Initialize the pagerankGraph with each edge attribute
    // having weight 1/outDegree and each vertex with attribute 0.
    val outDeg = graph.aggregateMessages[Float]({ t => t.sendToSrc(t.attr) }, _ + _)

    def maper(e: EdgeTriplet[Float, Float]): Float = e.attr / e.srcAttr

    val rankGraph = graph
      // Associhaate the degree with each vertex
      .outerJoinVertices(outDeg) { (_, _, deg) => deg.getOrElse(0.0f) }
      // Set the weight on the edges based on the degree
      .mapTriplets[Float](e => maper(e), TripletFields.All)
      // Set the vertex attributes to (initialPR, delta = 0)
      .mapVertices { (_, _) => (0.0f, 0.0f) }

    val initialMessage = resetProb / (1.0f - resetProb)

    def vertexProgram(id: VertexId, attr: (Float, Float), msgSum: Float): (Float, Float) = {
      val (oldPR, _) = attr
      val newPR = oldPR + (1.0f - resetProb) * msgSum
      (newPR, newPR - oldPR)
    }

    var iterGraph = rankGraph.mapVertices { case (id, attr) =>
      vertexProgram(id, attr, initialMessage)
    }.persist(storageLevel)
    var message = iterGraph.aggregateMessages[Float]({
      e =>
        if (e.srcAttr._2 > tol) e.sendToDst(e.srcAttr._2 * e.attr)
    }, _ + _
    ).persist(storageLevel)
    var numMsg = message.count()
    Log.withTimePrintln(s"[dynamic version of PageRank]iter=0, activeMessage=$numMsg")
    var it = 1
    while (numMsg > 0 && it <= maxIter) {
      val preGraph = iterGraph
      iterGraph = iterGraph.outerJoinVertices(message) {
        case (id, attr, Some(m)) => vertexProgram(id, attr, m)
        case (id, attr, None) => (attr._1, 0)
      }
      val preMessage = message
      message = iterGraph.aggregateMessages[Float]({
        e =>
          if (e.srcAttr._2 > tol) e.sendToDst(e.srcAttr._2 * e.attr)
      }, _ + _
      ).persist(storageLevel)
      if (it % checkpointInterval == 0) {
        iterGraph.checkpoint()
        message.checkpoint()
      }
      numMsg = message.count()
      Log.withTimePrintln(s"[dynamic version of PageRank]iter=$it, activeMessage=$numMsg")
      preGraph.unpersist(false)
      preMessage.unpersist(false)
      it += 1
    }
    // SPARK-18847 If the graph has sinks (vertices with no outgoing edges) correct the sum of ranks

//    iterGraph.vertices.map {case (id, rank) => Array(id.toString, rank._1.toString)}

    normalizeRankSum(iterGraph.mapVertices { case (_, (attr, _)) => attr })
  }

  private def normalizeRankSum(rankGraph: Graph[Float, Float]) = {
    val rankSum = rankGraph.vertices.values.sum()
    val correctionFactor = rankGraph.vertices.count() / rankSum.toFloat
    Log.withTimePrintln(s"running normalize, the correctionFactor: $correctionFactor")
    rankGraph.vertices.map { case (id, rank) => Array(id.toString, (rank * correctionFactor).toString) }
  }
}
