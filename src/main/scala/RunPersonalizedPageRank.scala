package ca.uwaterloo.cs651project

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.hadoop.fs._
import org.apache.spark.graphx._
import org.rogach.scallop._

class Conf(args: Seq[String]) extends ScallopConf(args) {
  mainOptions = Seq(input,output,sources,reducers)
  val input = opt[String](descr = "The graph", required = true)
  val output = opt[String](descr = "Where the output will be written", required = true)
  val sources = opt[String](descr = "The initial sources", required = true)
  val reducers = opt[Int](descr = "number of reducers", required = false, default = Some(1))
  verify()
}
  

object RunPersonalizedPageRank {
  def main(args: Array[String]): Unit = {
    println("Hello world!")
  }
}
