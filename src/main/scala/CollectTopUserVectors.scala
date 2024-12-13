package ca.uwaterloo.cs651project

import org.apache.log4j._
import org.apache.spark.ml.evaluation.RegressionEvaluator
import org.apache.spark.ml.recommendation.ALS
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.rogach.scallop._
import org.apache.spark.mllib.linalg.{Vector, Vectors}
import org.apache.spark.sql.functions.udf
import org.apache.spark._
import org.apache.spark.ml.linalg.DenseVector
import scala.collection.mutable.WrappedArray


import java.io.{BufferedWriter, FileWriter}
import java.nio.file.Paths
import scala.sys.process._
import scala.util.Random

class ConfCollectTopUserVectors(args: Seq[String]) extends ScallopConf(args) {
  mainOptions = Seq(size, modeldir, numberofusers, outputfile)
  val size = opt[String](descr = "MoviLens dataset (small/large)", required = false, default = Some("small"))
  val modeldir = opt[String](descr = "Where the model is stored", required = true)
  val numberofusers = opt[Int](descr = "Number of users", required = false, default = Some(100))
  val outputfile = opt[String](descr = "Text file where the vectors will be written", required = true)
  verify()
}

object CollectTopUserVectors {
  val log = Logger.getLogger(getClass.getName)

  private def getData(dataset: String): String = {
    val scriptPath = Paths.get("move_data_to_hdfs.sh").toAbsolutePath.toString
    val cmd = Seq(scriptPath, dataset)
    Process(cmd).!
    s"data_$dataset/ratings.csv"
  }

  def main(argv: Array[String]): Unit = {
    val args = new ConfCollectTopUserVectors(argv)
    val spark = SparkSession.builder()
      .appName("CollectTopUserVectors")
      .getOrCreate()

    log.info(s"Getting data (${args.size()} - this may take a while")
    val dataPath = getData(args.size())
	spark.conf.set("spark.driver.memory", "4g")
	spark.conf.set("spark.executor.memory", "4g")
import spark.implicits._



    val df = spark.read.option("header", "true").option("inferSchema", "true").csv(dataPath).select("movieId", "userId")
	
	
	// Generate the list of top users and broadcast it
	val topUsers = df.groupBy("userId")
	.agg(count("*").alias("count"))
	.orderBy(desc("count"))
	.limit(args.numberofusers())
	.select("userId")
	.map(row => row.getInt(0))  
	.collect
	

	val broadcastTopUsers = spark.sparkContext.broadcast(topUsers)
	val is_top_user = udf((value: Int) => broadcastTopUsers.value.contains(value))
	val filtered_df = df.filter(is_top_user($"userId"))
	
	val movie_vectors = spark.read.parquet(args.modeldir())
	.select("movieVector", "movieId")
	.join(filtered_df, Seq("movieId"), "inner")
	.map(row => (row.getAs[Int]("userId"), row.getAs[WrappedArray[Float]]("movieVector").toArray.map(_.toDouble)))
	.rdd
	.groupByKey
	.collectAsMap
	
	val outputfile : String = args.outputfile()
	val fileWriter = new BufferedWriter(new FileWriter(outputfile))
	movie_vectors.foreach(key => 
	{
		fileWriter.write("***\n")
		key._2.foreach(vector => fileWriter.write(vector.mkString(" ") + "\n"))
	})
	
    fileWriter.close()	

    spark.stop()
  }
}
