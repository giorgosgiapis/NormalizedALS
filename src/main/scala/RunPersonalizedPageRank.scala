package ca.uwaterloo.cs651project

import breeze.linalg.DenseVector
import breeze.stats.distributions.Multinomial
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.rogach.scallop._

import scala.sys.process._
import org.apache.log4j._

import java.nio.file.Paths
import scala.collection.mutable
import scala.util.Random

class Conf(args: Seq[String]) extends ScallopConf(args) {
  val log = Logger.getLogger(getClass.getName)
  private def getData: String = {
    log.info("Downloading data from MovieLens")
    val scriptPath = Paths.get("move_data_to_hdfs.sh").toAbsolutePath.toString
    Process(scriptPath).!
    "data/ratings.csv"
  }
  mainOptions = Seq(data)
  val data = opt[String](descr = "Path for the data", required = false, default = Some(getData))
  verify()
}


object MovieLens {
  val RATING_THRESHOLD = 120
  val MAX_ITER = 100000

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

    for (_ <- 1 to max_iter) {
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
    val args = new Conf(argv)
    val spark = SparkSession.builder()
      .appName("movielens")
      .config("spark.master", "local")
      .getOrCreate()

    import spark.implicits._

    val df = spark.read.option("header", "true").option("inferSchema", "true").csv(args.data())

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
    val Array(train, test) = movie_ratings_with_dist.randomSplit(Array(0.9, 0.1))

    val priors = train.select("distribution").rdd.map(row => row.getAs[Seq[Double]]("distribution")).collect()
    val tests = test.select("distribution").rdd.map(row => row.getAs[Seq[Double]]("distribution")).collect()

    val priors_bc = spark.sparkContext.broadcast(priors)
    val max_iter_bc = spark.sparkContext.broadcast(MAX_ITER)

    val ns = Seq(5, 10, 15, 20, 25)
    val n_x_pairs = for (n <- ns; x <- tests) yield (n, x)
    val n_x_rdd = spark.sparkContext.parallelize(n_x_pairs)

    def process_n_x_pair(n_x_pair: (Int, Seq[Double])): (Int, Double, Double) = {
      val (n, x) = n_x_pair
      val val_ = x.zip(1 to 5).map { case (x_i, r_i) => x_i * r_i }.sum
      val mult = Multinomial(DenseVector(x.toArray))
      val draw = mult.sample(n).toArray[Int]
      val sample = Array.fill(5)(0)
      for (i <- draw) {
        sample(i) += 1
      }
      val sample_mean = sample.zip(1 to 5).map { case (s_i, r_i) => s_i * r_i }.sum / n.toDouble
      val errors_1 = sample_mean - val_
      val priors = priors_bc.value
      val max_iter = max_iter_bc.value
      val posteriors = rejection_sampling(sample, priors, max_iter)
      val posterior_means = posteriors.map(p => p.zip(1 to 5).map { case (p_i, r_i) => p_i * r_i }.sum)
      val errors_2 = posterior_means.sum / posterior_means.length - val_
      (n, errors_1, errors_2)
    }

    val results = n_x_rdd.map(process_n_x_pair).collect()

    val errors_1_dict = mutable.Map[Int, Seq[Double]]().withDefaultValue(Seq.empty)
    val errors_2_dict = mutable.Map[Int, Seq[Double]]().withDefaultValue(Seq.empty)

    for ((n, e1, e2) <- results) {
      errors_1_dict(n) = errors_1_dict(n) :+ e1
      errors_2_dict(n) = errors_2_dict(n) :+ e2
    }

    val s_1 = ns.map { n =>
      val errors_1 = errors_1_dict.getOrElse(n, Seq.empty[Double])
      val mse_1 = errors_1.map(e => e * e).sum / errors_1.length
      (n, mse_1)
    }

    val s_2 = ns.map { n =>
      val errors_2 = errors_2_dict.getOrElse(n, Seq.empty[Double])
      val mse_2 = errors_2.map(e => e * e).sum / errors_2.length
      (n, mse_2)
    }

    println("Mean Squared Errors using Sample Mean Estimation (s_1):")
    s_1.foreach { case (n, mse) =>
      println(s"Sample Size $n: MSE = $mse")
    }

    println("\nMean Squared Errors using Rejection Sampling Estimation (s_2):")
    s_2.foreach { case (n, mse) =>
      println(s"Sample Size $n: MSE = $mse")
    }
  }
}
