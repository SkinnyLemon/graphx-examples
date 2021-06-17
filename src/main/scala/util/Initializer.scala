package de.htwg.rs.graphx
package util

import org.apache.spark.{SparkConf, SparkContext}

object Initializer {
  def initializeLocal(): SparkContext = {
    val conf = new SparkConf()
      .setMaster("local")
      .setAppName("Demonstration")
    new SparkContext(conf)
  }

  def initializeRemote(): SparkContext = {
    val conf = new SparkConf()
      .setJars(Seq("out/artifacts/graphx_examples_jar/graphx-examples.jar"))
      .setAppName("Demonstration")
      .setMaster("spark://development:7077")
      .set("spark.executor.memory", "8g")
      .set("spark.executor.cores", "6")
      .set("spark.executor.instances", "3")
    val sc = new SparkContext(conf)
    sc.addJar("out/artifacts/graphx_examples_jar/graphx-examples.jar")
    sc
  }
}
