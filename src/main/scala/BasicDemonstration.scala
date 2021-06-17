package de.htwg.rs.graphx

import util.Initializer

import org.apache.spark.SparkContext
import org.apache.spark.graphx.{Edge, Graph}

object BasicDemonstration {
  def main(args: Array[String]): Unit = {
    val sc = Initializer.initializeLocal()

    val graph = createFamily(sc)

    graph.triplets.map(triplet => s"${triplet.srcAttr} is the ${triplet.attr} of ${triplet.dstAttr}")
      .collect.foreach(println(_))
  }

  def createFamily(sc: SparkContext): Graph[String, String] = {
    val people = sc.parallelize(Seq(
      (1L, "Karl"),
      (2L, "Julia"),
      (3L, "Hans"),
      (4L, "Anne"),
      (5L, "Fritz"),
      (6L, "Hannelore"),
      (7L, "Paul"),
      (8L, "Gertrude")
    ))

    val relationships = sc.parallelize(Seq(
      Edge(1L, 2L, "brother"),
      Edge(2L, 1L, "sister"),
      Edge(3L, 1L, "father"),
      Edge(4L, 1L, "mother"),
      Edge(3L, 2L, "father"),
      Edge(4L, 2L, "mother"),
      Edge(3L, 4L, "husband"),
      Edge(4L, 3L, "wife"),
      Edge(5L, 3L, "father"),
      Edge(6L, 3L, "mother"),
      Edge(5L, 6L, "husband"),
      Edge(6L, 5L, "wife"),
      Edge(7L, 4L, "father"),
      Edge(8L, 4L, "mother"),
      Edge(7L, 8L, "husband"),
      Edge(8L, 7L, "wife"),
    ))

    Graph(people, relationships)
  }
}
