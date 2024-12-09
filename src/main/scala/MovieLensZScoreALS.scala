package ca.uwaterloo.cs651project

import org.apache.log4j._
import org.apache.spark.ml.evaluation.RegressionEvaluator
import org.apache.spark.ml.recommendation.ALS
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.rogach.scallop._

import java.io.{BufferedWriter, FileWriter}
import java.nio.file.Paths
import scala.sys.process._
import scala.util.Random

class ConfMovieLensZScoreALS(args: Seq[String]) extends ScallopConf(args) {
  mainOptions = Seq(size, rank, runs)
  val size = opt[String](descr = "MoviLens dataset (small/large)", required = false, default = Some("small"))
  val rank = opt[Int](descr = "ALS rank", required = false, default = Some(6))
  val runs = opt[Int](descr = "Number of runs for ALS", required = false, default = Some(1))
  verify()
}

object MovieLensZScoreALS {
  val log = Logger.getLogger(getClass.getName)
  val RATING_THRESHOLD = 110
  val MAX_ITER = 100000
  val SAMPLE_THRESH = 45

  private def getData(dataset: String): String = {
    val scriptPath = Paths.get("move_data_to_hdfs.sh").toAbsolutePath.toString
    val cmd = Seq(scriptPath, dataset)
    Process(cmd).!
    s"data_$dataset/ratings.csv"
  }

  private def stddev(seq: Seq[Double]): Double = {
    val mean = seq.sum / seq.length
    math.sqrt(seq.map(x => math.pow(x - mean, 2)).sum / seq.length)
  }

  private def rejection_sampling(stats: Seq[Int], priors: Array[Seq[Double]], max_iter: Int, tol: Double = 0.1): Seq[Seq[Double]] = {
    var dists = Seq.empty[Seq[Double]]
    val sum_stats = stats.sum.toDouble
    val stats_length = stats.length
    var samples = 0
    val m2 = Array(0.0, 0.0)
    val cur_mean = Array(0.0, 0.0)
    val cur_var = Array(0.0, 0.0)
    val random = new Random()

    for (iter <- 1 to max_iter) {
      val q = priors(random.nextInt(priors.length))
      val alpha = (0 until stats_length).map { i =>
        if (stats(i) == 0) 0.0
        else stats(i) * math.log(q(i) * sum_stats / stats(i))
      }.sum
      if (alpha > math.log(random.nextDouble())) {
        dists = dists :+ q
        samples += 1
        val stat_list = Array(q.sum / q.length, stddev(q))
        for (i <- 0 until 2) {
          val cur = stat_list(i)
          val new_mean = cur_mean(i) + (cur - cur_mean(i)) / samples
          m2(i) = m2(i) + (cur - cur_mean(i)) * (cur - new_mean)
          cur_mean(i) = new_mean
          cur_var(i) = m2(i) / samples
        }
        if (samples >= 100 && cur_var.forall(_ < tol * samples)) {
          return dists
        }
      }
    }
    dists
  }

  def main(argv: Array[String]): Unit = {
    val args = new ConfMovieLensZScoreALS(argv)
    val spark = SparkSession.builder()
      .appName("MovieLensZScoreALS")
      .getOrCreate()

    val runs = args.runs()
    log.info(s"Getting data (${args.size()} - this may take a while")
    val dataPath = getData(args.size())

    import spark.implicits._

    val df = spark.read.option("header", "true").option("inferSchema", "true").csv(dataPath)

    // Trusted movies
    val counts = df.groupBy("movieId").agg(count("*").alias("count"))
    val reduced_counts = counts.filter($"count" > RATING_THRESHOLD)
    val reduced_df = df.join(reduced_counts, Seq("movieId"), "inner").drop("timestamp")

    val movie_ratings = reduced_df.groupBy("movieId")
      .agg(
        collect_list("rating").alias("ratings"),
        count("*").alias("count")
      )

    val process_ratings_udf = udf((ratings: Seq[Double]) => {
      val filtered = ratings.filter(_ >= 1)
      val noisy = filtered.map(r => math.round(r + Random.nextGaussian() * 0.1).toInt)
      noisy
    }, ArrayType(IntegerType))

    val movie_ratings_processed = movie_ratings.withColumn("processed_ratings", process_ratings_udf($"ratings"))

    val create_distribution_udf = udf((ratings: Seq[Int], count: Long) => {
      val smoothed_ratings = ratings ++ Seq(1, 2, 3, 4, 5)
      val countsMap = smoothed_ratings.groupBy(identity).mapValues(_.size.toDouble)
      val total = smoothed_ratings.size.toDouble
      val probs = (1 to 5).map(i => countsMap.getOrElse(i, 0.0) / total)
      probs
    }, ArrayType(DoubleType))

    val movie_ratings_with_dist = movie_ratings_processed.withColumn("distribution", create_distribution_udf($"processed_ratings", $"count"))

    val priors = movie_ratings_with_dist.select("distribution").rdd.map(row => row.getAs[Seq[Double]]("distribution")).collect()
    val priors_bc = spark.sparkContext.broadcast(priors)

    val movie_ratings_all = df.groupBy("movieId")
      .agg(
        collect_list("rating").alias("ratings"),
        count("*").alias("count")
      )
      .withColumn("processed_ratings", process_ratings_udf($"ratings"))
      .withColumn("distribution", create_distribution_udf($"processed_ratings", $"count"))

    // If count >= SAMPLE_THRESH, use sample mean/std from processed_ratings.
    // Otherwise use rejection sampling to get posterior distributions, then derive mean/std from them.
    val movieStats = movie_ratings_all.rdd.map { row =>
      val movieId = row.getAs[Int]("movieId")
      val ratings = row.getAs[Seq[Int]]("processed_ratings")
      val cnt = row.getAs[Long]("count")
      if (cnt >= SAMPLE_THRESH) {
        val mean = ratings.sum.toDouble / ratings.size
        val variance = ratings.map(r => (r - mean) * (r - mean)).sum / ratings.size
        val std = math.sqrt(variance)
        (movieId, mean, std)
      } else {
        val countsMap = ratings.groupBy(identity).mapValues(_.size)
        val counts = (1 to 5).map(i => countsMap.getOrElse(i, 0))
        val posteriors = rejection_sampling(counts, priors_bc.value, MAX_ITER)
        if (posteriors.isEmpty) {
          val mean = ratings.sum.toDouble / ratings.size
          val std = stddev(ratings.map(_.toDouble))
          (movieId, mean, std)
        } else {
          val posterior_stats = posteriors.map { dist =>
            val mean_dist = dist.zipWithIndex.map { case (p, i) => p * (i + 1) }.sum
            val variance_dist = dist.zipWithIndex.map { case (p, i) => p * math.pow((i + 1) - mean_dist, 2) }.sum
            (mean_dist, math.sqrt(variance_dist))
          }
          val avg_mean = posterior_stats.map(_._1).sum / posterior_stats.size
          val avg_std = posterior_stats.map(_._2).sum / posterior_stats.size
          (movieId, avg_mean, if (avg_std == 0.0) 1e-9 else avg_std)
        }
      }
    }.toDF("movieId", "mean", "std")

    val joined_df = df.join(movieStats, Seq("movieId"), "left")

    val zscore_udf = udf((rating: Double, mean: Double, std: Double) => {
      if (std == 0.0) rating - mean else (rating - mean) / std
    })

    val inverse_transform_udf = udf((prediction: Float, mean: Double, std: Double) => {
      prediction.toDouble * std + mean
    })

    val normalized_df = joined_df.withColumn("normalized_rating", zscore_udf($"rating", $"mean", $"std"))
      .select("userId", "movieId", "normalized_rating", "rating", "mean", "std")
      .cache()

    val ratio = if (args.size() == "small") 0.8 else 0.9
    val rank = args.rank()

    val als = new ALS()
      .setRank(rank)
      .setMaxIter(25)
      .setRegParam(0.2)
      .setUserCol("userId")
      .setItemCol("movieId")
      .setRatingCol("normalized_rating")
      .setColdStartStrategy("drop")

    val evaluator = new RegressionEvaluator()
      .setMetricName("mse")
      .setLabelCol("rating")
      .setPredictionCol("raw_predictions")

    var losses = List[Double]()
    for (run <- 1 to runs) {
      log.info(s"ALS Run $run")
      val Array(training, testing) = normalized_df.randomSplit(Array(ratio, 1 - ratio))
      val model = als.fit(training)
      val predictions = model.transform(testing)
      val raw_predictions = predictions
        .withColumn("raw_predictions", inverse_transform_udf($"prediction", $"mean", $"std"))
      val mse = evaluator.evaluate(raw_predictions)
      losses = losses :+ mse
    }

    log.info(s"Writing losses to normalized_losses_rank$rank.txt")
    val filePath = s"normalized_losses_rank$rank.txt"
    val fileWriter = new BufferedWriter(new FileWriter(filePath))
    fileWriter.write(losses.mkString("\n"))
    fileWriter.close()

    spark.stop()
  }
}
