name := "graphx-examples"
version := "0.1"
scalaVersion := "2.12.10"
idePackagePrefix := Some("de.htwg.rs.graphx")

libraryDependencies += "org.apache.spark" % "spark-core_2.12" % "3.1.2" % "provided"
libraryDependencies += "org.apache.spark" % "spark-graphx_2.12" % "3.1.2" % "provided"
libraryDependencies += "org.apache.spark" % "spark-sql_2.12" % "3.1.2" % "provided"

libraryDependencies += "org.neo4j" % "neo4j-connector-apache-spark_2.12" % "4.0.2_for_spark_3"

run in Compile := Defaults.runTask(fullClasspath in Compile, mainClass in (Compile, run), runner in (Compile, run)).evaluated
runMain in Compile := Defaults.runMainTask(fullClasspath in Compile, runner in(Compile, run)).evaluated