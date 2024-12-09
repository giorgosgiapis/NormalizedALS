package ca.uwaterloo.cs651project

import breeze.linalg.DenseVector
import breeze.stats.distributions.Multinomial
import org.apache.log4j._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.rogach.scallop._

import java.io.{BufferedWriter, FileWriter}
import java.nio.file.Paths
import scala.collection.mutable
import scala.sys.process._
import scala.util.Random

class ConfRejectionSamplingPlotsData(args: Seq[String]) extends ScallopConf(args) {
  mainOptions = Seq(size)
  val size = opt[String](descr = "MoviLens dataset (small/large)", required = false, default = Some("small"))
  verify()
}


object RejectionSamplingPlotsData {
  val log = Logger.getLogger(getClass.getName)
  val RATING_THRESHOLD = 120
  val MAX_ITER = 100000

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

  private def dist_stddev(p: Seq[Double]): Double = {
    val mean = p.zipWithIndex.map { case (prob, i) => prob * (i + 1) }.sum
    val var_ = p.zipWithIndex.map { case (prob, i) => prob * math.pow((i + 1) - mean, 2) }.sum
    math.sqrt(var_)
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
          log.info(s"Converged after $iter iterations")
          return dists
        }
      }
    }
    dists
  }

  def main(argv: Array[String]): Unit = {
    val args = new ConfRejectionSamplingPlotsData(argv)
    val spark = SparkSession.builder()
      .appName("RejectionSamplingPlotsData")
      .getOrCreate()

    log.info(s"Getting data (${args.size} - this may take a while")
    val dataPath = getData(args.size())

    import spark.implicits._

    val df = spark.read.option("header", "true").option("inferSchema", "true").csv(dataPath)

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

    def process_n_x_pair(n_x_pair: (Int, Seq[Double])): (Int, Double, Double, Double, Double) = {
      val (n, x) = n_x_pair
      // True mean and stddev of the distribution x
      val true_mean = x.zip(1 to 5).map { case (x_i, r_i) => x_i * r_i }.sum
      val true_std = dist_stddev(x)

      // Sample from the multinomial distribution
      val mult = Multinomial(DenseVector(x.toArray))
      val draw = mult.sample(n).toArray[Int]
      val sample = Array.fill(5)(0)
      for (i <- draw) {
        sample(i) += 1
      }
      val sample_mean = sample.zip(1 to 5).map { case (s_i, r_i) => s_i * r_i }.sum / n.toDouble
      val sample_std = dist_stddev(sample.map(_.toDouble / n.toDouble))

      // Errors for sample mean estimation
      val mean_err_1 = sample_mean - true_mean
      val std_err_1 = sample_std - true_std

      // Rejection sampling
      val priors = priors_bc.value
      val max_iter = max_iter_bc.value
      val posteriors = rejection_sampling(sample, priors, max_iter)
      val posterior_means = posteriors.map(p => p.zip(1 to 5).map { case (p_i, r_i) => p_i * r_i }.sum)
      val posterior_mean = posterior_means.sum / posterior_means.length
      val mean_err_2 = posterior_mean - true_mean

      val posterior_stds = posteriors.map(dist_stddev)
      val posterior_std_mean = posterior_stds.sum / posterior_stds.length
      val std_err_2 = posterior_std_mean - true_std

      (n, mean_err_1, mean_err_2, std_err_1, std_err_2)
    }

    val results = n_x_rdd.map(process_n_x_pair).collect()

    // Errors 1 => Sample Mean Estimation
    // Errors 2 => Rejection Sampling Estimation
    val mean_errors_1_dsct = mutable.Map[Int, Seq[Double]]().withDefaultValue(Seq.empty)
    val mean_errors_2_dict = mutable.Map[Int, Seq[Double]]().withDefaultValue(Seq.empty)
    val std_errors_1_dict = mutable.Map[Int, Seq[Double]]().withDefaultValue(Seq.empty)
    val std_errors_2_dict = mutable.Map[Int, Seq[Double]]().withDefaultValue(Seq.empty)

    for ((n, me1, me2, se1, se2) <- results) {
      mean_errors_1_dsct(n) = mean_errors_1_dsct(n) :+ me1
      mean_errors_2_dict(n) = mean_errors_2_dict(n) :+ me2
      std_errors_1_dict(n) = std_errors_1_dict(n) :+ se1
      std_errors_2_dict(n) = std_errors_2_dict(n) :+ se2
    }

    val s_1_mean = ns.map { n =>
      val mean_errors_1 = mean_errors_1_dsct.getOrElse(n, Seq.empty[Double])
      val mse_1_mean = mean_errors_1.map(e => e * e).sum / mean_errors_1.length
      (n, mse_1_mean)
    }

    val s_2_mean = ns.map { n =>
      val mean_errors_2 = mean_errors_2_dict.getOrElse(n, Seq.empty[Double])
      val mse_2_mean = mean_errors_2.map(e => e * e).sum / mean_errors_2.length
      (n, mse_2_mean)
    }

    val s_1_std = ns.map { n =>
      val std_errors_1 = std_errors_1_dict.getOrElse(n, Seq.empty[Double])
      val mse_1_std = std_errors_1.map(e => e * e).sum / std_errors_1.length
      (n, mse_1_std)
    }

    val s_2_std = ns.map { n =>
      val std_errors_2 = std_errors_2_dict.getOrElse(n, Seq.empty[Double])
      val mse_2_std = std_errors_2.map(e => e * e).sum / std_errors_2.length
      (n, mse_2_std)
    }

    var filePath = "sample_mean_mse_mean.txt"
    var fileWriter = new BufferedWriter(new FileWriter(filePath))
    log.info("Writing Mean Squared Errors for mean using Sample Mean Estimation")
    s_1_mean.foreach { case (n, mse) =>
      fileWriter.write(s"$n\t$mse\n")
    }
    fileWriter.close()

    filePath = "rejection_sampling_mse_mean.txt"
    fileWriter = new BufferedWriter(new FileWriter(filePath))
    log.info("Writing Mean Squared Errors for mean using Rejection Sampling Estimation")
    s_2_mean.foreach { case (n, mse) =>
      fileWriter.write(s"$n\t$mse\n")
    }
    fileWriter.close()

    filePath = "sample_mean_mse_std.txt"
    fileWriter = new BufferedWriter(new FileWriter(filePath))
    log.info("Writing Mean Squared Errors for std using Sample Mean Estimation")
    s_1_std.foreach { case (n, mse) =>
      fileWriter.write(s"$n\t$mse\n")
    }
    fileWriter.close()

    filePath = "rejection_sampling_mse_std.txt"
    fileWriter = new BufferedWriter(new FileWriter(filePath))
    log.info("Writing Mean Squared Errors for std using Rejection Sampling Estimation")
    s_2_std.foreach { case (n, mse) =>
      fileWriter.write(s"$n\t$mse\n")
    }
    fileWriter.close()

    spark.stop()
  }
}
