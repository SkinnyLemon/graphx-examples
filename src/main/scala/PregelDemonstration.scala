package de.htwg.rs.graphx

import util.Initializer

import org.apache.spark.graphx.{EdgeContext, EdgeDirection, EdgeTriplet, VertexId}

object PregelDemonstration {
  def main(args: Array[String]): Unit = {
    val sc = Initializer.initializeLocal()
    val descAttr = Seq("father", "mother")

    val family = BasicDemonstration.createFamily(sc)
      .filter[String, String](
        preprocess = g => g,
        epred = triplet => descAttr.contains(triplet.attr))
      .reverse
      .mapVertices((_, name) => (name, 0))


    def messageReceive(id: VertexId, attribute: (String, Int), message: Int): (String, Int) = {
      (attribute._1, message)
    }

    def messageSend(triplet: EdgeTriplet[(String, Int), String]): Iterator[(VertexId, Int)] =
      Iterator((triplet.dstId, triplet.srcAttr._2 + 1))

    def combine(m1: Int, m2: Int): Int = m1 + m2


    val descendants = family
      .pregel(
        initialMsg = 0,
        activeDirection = EdgeDirection.Out
      )(
        vprog = messageReceive,
        sendMsg = messageSend,
        mergeMsg = combine
      )


    descendants.vertices.map { case (_, (name, n)) => s"$name has $n descendants" }
      .collect().foreach(println)
  }
}
