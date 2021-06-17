package de.htwg.rs.graphx
package misc

import org.apache.spark.graphx.{Edge, Graph}
import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}

object WikiDemonstration {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setJars(Seq("out/artifacts/graphx_examples_jar/graphx-examples.jar"))
      .setAppName("Demonstration")
      .setMaster("spark://development:7077")
      .set("spark.executor.memory", "16g")
      .set("spark.executor.cores", "12")
      .set("spark.executor.instances", "3")
      .set("spark.network.timeout", "10000000")
    val sc = new SparkContext(conf)
    sc.addJar("out/artifacts/graphx_examples_jar/graphx-examples.jar")

    val spark = SparkSession.builder.config(conf).getOrCreate()

    val pages = spark.read.format("org.neo4j.spark.DataSource")
      .option("url", "bolt://development:7687")
      .option("database", "wiki")
      .option("authentication.type", "basic")
      .option("authentication.basic.username", "neo4j")
      .option("authentication.basic.password", "test")
      .option("labels", "Page")
      .option("partition", "10")
      .load()
      .repartition(10)
      .rdd
      .map(row =>
        (row.getAs[Long]("<id>"), row.getAs[String]("title")))

    val links = spark.read.format("org.neo4j.spark.DataSource")
      .option("url", "bolt://development:7687")
      .option("database", "wiki")
      .option("authentication.type", "basic")
      .option("authentication.basic.username", "neo4j")
      .option("authentication.basic.password", "test")
      .option("relationship", "Link")
      .option("relationship.nodes.map", "false")
      .option("relationship.source.labels", "Page")
      .option("relationship.target.labels", "Page")
      .option("partition", "100")
      .load()
      .repartition(100)
      .rdd
      .map(row =>
        Edge(row.getAs[Long]("<source.id>"), row.getAs[Long]("<target.id>"), row.getAs[String]("<rel.type>")))

    val graph = Graph(pages, links)

    val ranks = graph.pageRank(0.01).vertices

    val pagesByName = pages.join(ranks).map {
      case (id, (name, rank)) => (name, rank)
    }

    println(pagesByName.sortBy { case (_, rank) => -rank }.take(10).mkString("\n"))
  }
}
