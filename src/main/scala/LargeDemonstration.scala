package de.htwg.rs.graphx

import util.Initializer

import org.apache.spark.graphx.util.GraphGenerators

object LargeDemonstration {
  def main(args: Array[String]): Unit = {
    val sc = Initializer.initializeRemote()

    val graph = GraphGenerators.logNormalGraph(sc, 1000000, 20, 5.0)
      .cache()

    val center = graph.pageRank(0.001)
      .vertices
      .sortBy({ case (_, weight) => weight }, false)
      .first()
      ._1

    val mostDistant = graph
      .mapVertices((id, _) =>
        if (id == center) 0
        else Long.MaxValue - 10)
      .pregel(Long.MaxValue)(
        (_, distance, message) => {
          Math.min(distance, message)
        },
        triplet => {
          if (triplet.srcAttr + 1 < triplet.dstAttr) Iterator((triplet.dstId, 1 + triplet.srcAttr))
          else Iterator.empty
        },
        Math.min)
      .vertices
      .map(_._2)
      .filter(_ < Long.MaxValue - 10)
      .sortBy(n => n, ascending = false)
      .take(20)
      .mkString(", ")

    println(mostDistant)
  }
}
