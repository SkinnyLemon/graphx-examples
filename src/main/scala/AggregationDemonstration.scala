package de.htwg.rs.graphx

import util.Initializer

import org.apache.spark.graphx.PartitionStrategy

object AggregationDemonstration {
  def main(args: Array[String]): Unit = {
    val sc = Initializer.initializeLocal()

    val family = BasicDemonstration.createFamily(sc)
      .partitionBy(PartitionStrategy.CanonicalRandomVertexCut)
      .groupEdges((_, _) => "knows")
      .convertToCanonicalEdges()
      .mapVertices((_, name) => (name, 0))

    val acquaintances = family.aggregateMessages[(String, Int)](
      context => {
        context.sendToDst(context.dstAttr._1, 1)
        context.sendToSrc(context.srcAttr._1, 1)
      },
      (a, b) => (a._1, a._2 + b._2)
    )

    acquaintances.map { case (_, (name, n)) => s"$name has $n acquaintances" }
      .collect().foreach(println)
  }
}
