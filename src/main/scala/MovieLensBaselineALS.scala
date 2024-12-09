package ca.uwaterloo.cs651project

import org.apache.log4j._
import org.apache.spark.ml.evaluation.RegressionEvaluator
import org.apache.spark.ml.recommendation.ALS
import org.apache.spark.sql.SparkSession
import org.rogach.scallop._

import java.io.{BufferedWriter, FileWriter}
import java.nio.file.Paths
import scala.sys.process._

class ConfBaseline(args: Seq[String]) extends ScallopConf(args) {
  mainOptions = Seq(size, runs)
  val size = opt[String](descr = "MoviLens dataset (small/large)", required = false, default = Some("small"))
  val runs = opt[Int](descr = "Number of runs", required = false, default = Some(1))
  verify()
}


object MovieLensBaselineALS {
  val log = Logger.getLogger(getClass.getName)

  private def getData(dataset: String): String = {
    val scriptPath = Paths.get("move_data_to_hdfs.sh").toAbsolutePath.toString
    val cmd = Seq(scriptPath, dataset)
    Process(cmd).!
    s"data_$dataset/ratings.csv"
  }

  def main(argv: Array[String]): Unit = {
    val args = new ConfBaseline(argv)
    val spark = SparkSession.builder()
      .appName("MovieLensBaselineALS")
      .getOrCreate()

    log.info(s"Getting data (${args.size} - this may take a while")
    val dataPath = getData(args.size())

    val df = spark.read.option("header", "true").option("inferSchema", "true").csv(dataPath)
    val ratings = df.select("userId", "movieId", "rating").cache()
    val ratio = if (args.size() == "small") 0.8 else 0.9
    val rank = if (args.size() == "small") 6 else 10
    val losses = spark.sparkContext.collectionAccumulator[Double]("Losses")

    val ALS = new ALS()
      .setRank(rank)
      .setMaxIter(25)
      .setRegParam(0.1)
      .setUserCol("userId")
      .setItemCol("movieId")
      .setRatingCol("rating")
      .setColdStartStrategy("drop")

    for (run <- 1 to args.runs()) {
      log.info(s"ALS Run $run")
      val Array(training, test) = ratings.randomSplit(Array(ratio, 1 - ratio))
      val model = ALS.fit(training)
      val predictions = model.transform(test)

      val evaluator = new RegressionEvaluator()
        .setMetricName("mse")
        .setLabelCol("rating")
        .setPredictionCol("prediction")
      val mse = evaluator.evaluate(predictions)
      losses.add(mse)
    }
    val loss_list = losses.value.toArray
    log.info("Writing losses to baseline_losses.txt")
    val filePath = "baseline_losses.txt"
    val fileWriter = new BufferedWriter(new FileWriter(filePath))
    fileWriter.write(loss_list.mkString("\n"))
    fileWriter.close()
  }
}
