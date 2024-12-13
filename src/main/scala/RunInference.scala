package ca.uwaterloo.cs651project

import org.apache.log4j._
import org.apache.spark.ml.regression.LinearRegression
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.rogach.scallop._
import org.apache.spark.ml.linalg.{Vector, Vectors}
import org.apache.spark.sql.functions.udf
import org.apache.spark._
import org.apache.spark.ml.feature.BucketedRandomProjectionLSH
import org.apache.spark.sql.Row
import org.apache.spark.sql.expressions.Window
import org.apache.spark.ml.linalg.DenseVector
import scala.collection.mutable.WrappedArray



import java.io.{BufferedWriter, FileWriter}
import java.nio.file.Paths
import scala.sys.process._
import scala.util.Random

class ConfRunInference(args: Seq[String]) extends ScallopConf(args) {
  mainOptions = Seq(size, modeldir, userids, bval, number)
  val size = opt[String](descr = "MoviLens dataset (small/large)", required = false, default = Some("small"))
  val modeldir = opt[String](descr = "Where the model is stored", required = true)
  val userids = opt[String](descr = "User IDs, separated by a comma", required = true)
  val bval       = opt[Double](descr = "The value of the diversification parameter", required = false, default = Some(0.5))
  val number       = opt[Int](descr = "The number of recommendations to be made for every user", required = false, default = Some(5))
  verify()
}

object RunInference {
  val log = Logger.getLogger(getClass.getName)
  val g = (t: Double) => -17.647 + 171.865*t - 664.272*t*t + 1273.414*t*t*t - 1210.543*t*t*t*t + 456.619*t*t*t*t*t
  val cutoff = 0.4
  val epsilon = 1e-05
  val d_0 = math.sqrt(2-2*cutoff)

  private def getData(dataset: String): String = {
    val scriptPath = Paths.get("move_data_to_hdfs.sh").toAbsolutePath.toString
    val cmd = Seq(scriptPath, dataset)
    Process(cmd).!
    s"data_$dataset"
  }

  def main(argv: Array[String]): Unit = {
    val args = new ConfRunInference(argv)
    val spark = SparkSession.builder()
      .appName("RunInference")
      .getOrCreate()

    log.info(s"Getting data (${args.size()} - this may take a while")
    val dataPath = getData(args.size())
	spark.conf.set("spark.driver.memory", "4g")
	spark.conf.set("spark.executor.memory", "4g")
	import spark.implicits._

	// Parse the users
	val users = args.userids()
	.split(",")
	.map(x=>x.toInt)
	.toSet
	.toArray
	.sorted
	
	val user_indices = Map((0 until users.length).map(i => (users(i),i)): _*) // Map userId -> index in users
	
	val B = args.bval()
	
    val df = spark.read.option("header", "true")
	.option("inferSchema", "true").csv(dataPath + "/ratings.csv")
	
	// Check that all the user Ids that were given are in the database
	users.foreach(user => {require(df.filter($"userId" === user).count() > 0, "User id " + user + " not found")})
	
	val unpack_vector = udf((vector: WrappedArray[Float]) => new DenseVector(vector.toArray.map(_.toDouble)))

	// Load the movie vectors
	val movie_vectors = spark.read.parquet(args.modeldir())
	.withColumn("movieVector", unpack_vector($"movieVector"))
	
	val is_good_user = udf((user: Int) => users.contains(user)) 

	val normalize_rating = udf((rating: Double, mean: Double, std: Double) => (rating - mean)/std)

	val ratings = df.select("movieId", "userId", "rating")
	val ratings_with_vectors = ratings.join(movie_vectors, Seq("movieId"), "inner")
	
	
	val filtered_ratings_with_vectors = ratings_with_vectors.filter(is_good_user($"userId"))
	
	/**** Computing the user vectors ****/
	val user_vectors : Array[DenseVector] = users.map(user => 
	{
		val user_df = filtered_ratings_with_vectors
		.filter($"userId" === user)
		.withColumn("normalizedRating", normalize_rating($"rating", $"mean", $"std"))
		.select("movieVector", "normalizedRating")
		

		new DenseVector(new LinearRegression()
		.setMaxIter(20)  
		.setRegParam(0.2)
		.setFitIntercept(false)
		.setLabelCol("normalizedRating")
		.setFeaturesCol("movieVector")
		.fit(user_df)
		.coefficients
		.toArray)
	})
		
	/**** Computing the diversification scores and scoring the movies ****/
	// Collecting the movie ids corresponding to movies that were rated by the users we were given
	val rated_user_movie_pairs = filtered_ratings_with_vectors.select("movieId", "userId")
	val rated_movie_IDs = rated_user_movie_pairs.select("movieId").distinct

	// Normalize the pdfs by collecting the number of ratings for every user
	val  rating_counts = filtered_ratings_with_vectors.groupBy("userId")
	.agg(count("*").alias("count"))
	.map(row => (row.getAs[Int]("userId"), row.getAs[Long]("count").toInt))
	.rdd
	.collectAsMap
	val rating_counts_udf = udf((x : Int) => rating_counts(x))
	
	val renormalize_vector = udf((vector: DenseVector) => 	{
		val norm = Vectors.norm(vector, 2.0) 
		Vectors.dense(vector.toArray.map(_ / norm))
	})
	
	val normalized_movie_vectors = movie_vectors.withColumn("normalizedMovieVector", renormalize_vector(col("movieVector")))
	 // Take the whole collection of normalized movie vectors and filter out the ones which weren't rated by a user in the provided list
	val normalized_rated_movie_vectors = normalized_movie_vectors.join(rated_movie_IDs, Seq("movieId"), "inner")
	
	// Locality sensitive hashing
	val model = new BucketedRandomProjectionLSH()
	.setInputCol("normalizedMovieVector")
	.setOutputCol("hashes")
	.setBucketLength(d_0)  
	.setNumHashTables(5)
	.fit(normalized_movie_vectors)
	
	val dot_product = (vector1: DenseVector, vector2: DenseVector) => 
	{
		val values1 = vector1.toArray
		val values2 = vector2.toArray
		(0 until values1.length).map(i => values1(i) * values2(i)).sum
	}
	val dot_product_udf = udf(dot_product)
	
	
	val window = Window.partitionBy("userId").orderBy(col("movieScore").desc)
	val N = args.number()
	val g_udf = udf(g)
	
	val predict_movie_rating = (userId: Int, movieVector: DenseVector, moviemean: Double, moviestd: Double) => Math.min(Math.max(Math.round(moviemean + moviestd* dot_product(user_vectors(user_indices(userId)), movieVector)*2)/2, 1.0), 5.0)
	val score_movie = udf((userId: Int, movieVector: DenseVector, pdf: Double, moviemean: Double, moviestd: Double) => predict_movie_rating(userId, movieVector, moviemean, moviestd) - B * Math.log(pdf + epsilon))
	
	
	val movie_scores = model.approxSimilarityJoin(model.transform(normalized_rated_movie_vectors), model.transform(normalized_movie_vectors), d_0)
	.withColumn("dot_product", dot_product_udf(col("datasetA.normalizedMovieVector"), col("datasetB.normalizedMovieVector")))
	.withColumn("g_value", when(col("dot_product") >= cutoff, g_udf($"dot_product")).otherwise(0.0))
	.withColumn("ratedMovieId", col("datasetA.movieId"))
	.withColumn("movieId", col("datasetB.movieId"))
	.withColumn("movieVector", col("datasetB.movieVector"))
	.select("g_value", "ratedMovieId", "movieId", "movieVector")
	// After the next step, the df contains movieId, ratedMovieId, userId, g_value, movieVector 
	.join(filtered_ratings_with_vectors.select("movieId", "userId").withColumnRenamed("movieId", "ratedMovieId"), Seq("ratedMovieId"), "inner") 
	.select("userId", "movieId", "movieVector", "ratedMovieId", "g_value")
	// Add a dummy entry for every movie/user pair so they are all present in the resulting df after we reduce over g_value
	.union(users.toSeq.toDF("userId").crossJoin(movie_vectors).withColumn("g_value", lit(0.0)).withColumn("ratedMovieId", lit(0)).select("userId", "movieId", "movieVector", "ratedMovieId", "g_value"))
	// We use a left join to delete the user/movie pairs which correspond to known ratings (and which the user has already seen)
	.join(rated_user_movie_pairs.withColumn("alreadyExists", lit(true)), Seq("userId", "movieId"), "left")
	.filter(col("alreadyExists").isNull)
	// Next, we sum over all the g_values for every user/movie pair and normalize
	.groupBy("userId", "movieId", "movieVector")
	.agg(sum("g_value").alias("unnormalized_pdf"))
	.withColumn("pdf", col("unnormalized_pdf") / rating_counts_udf($"userId"))
	.join(movie_vectors.drop("movieVector"), Seq("movieId"), "inner") // Inject back means and stds
	.withColumn("movieScore", score_movie($"userId", $"movieVector", $"pdf", $"mean", $"std"))
	.select("userId", "movieId", "movieScore")
	.withColumn("rowNumber", row_number().over(window))
	.filter($"rowNumber" <= N)
	.drop("rowNumber")
	
	// Join with the movies dataframe to fetch the actual movie titles
	val picks = spark.read.option("header", "true")
	.option("inferSchema", "true")
	.csv(dataPath + "/movies.csv")
	.drop("genres")
	.join(movie_scores, Seq("movieId"), "right") // Annoyingly, the movie database has some holes, so we need to do a right join
	.select("userId", "title", "movieScore")
	.map(row => (row.getAs[Int]("userId"), Option(row.getAs[String]("title")).getOrElse("?"), row.getAs[Double]("movieScore")))
	.rdd
	.collect
	
	users.foreach(user => {
		println(s"Movie recommendations for user $user:")
		val sorted_picks = picks.filter(pair=>pair._1==user).sortBy(-_._3)
		(1 to N).foreach(i => {println("" + i + ". " + sorted_picks(i-1)._2 + " (score=" + sorted_picks(i-1)._3 + ")")})
		println("---")
	})
	
	
  }
}
