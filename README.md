## ALS with Rating Normalization

This repository contains the Scala code for the final project of the course **CS651: Data-Intensive Distributed Computing** 
at the University of Waterloo, for the Fall 2024 term. You can find the report for this project [here]().


### Requirements:
The Java, Scala and Spark versions used here are the same as the ones 
used in the course throughout the Fall 2024 term. Namely:
- Java: 1.8.0
- Scala: 2.11.8
- Spark: 2.3.1
- Maven (any reasonably recent version should work)

To compile the project, first run `mvn clean package`.

### How to run:
To run the code used to generate the data to evaluate the rejection sampling method 
run:
```
 spark-submit --class ca.uwaterloo.cs651project.RejectionSamplingPlotsData target/project-1.0.jar --size [small/large]
```
where the `--size` indicates the version of the dataset used (`small` or `large`). This argument is optional. If not provided, 
the small dataset will be used.
This code will generate 5 text files, four of them containing the test MSE for the mean and standard deviation across various number of 
samples (the data for Fig. 2 in the report) and the last one containing the average number of generated samples in the rejection sampling 
across various numbers of samples (the data for Fig. 3 in the report).

To the Baseline ALS model run:
```
spark-submit --class ca.uwaterloo.cs651project.MovieLensBaselineALS target/project-1.0.jar --size [small/large] --rank [factors_rank] --runs [no_of_runs]
```
where the `--size` is as before  (defaults to `small`). The `--rank` arguments dictates the rank of the factor matrices (defaults to 6).
The `--runs` argument is optional and indicates the number of runs. If not provided, it defaults to 1. The test MSE for 
each run of the baseline algorithm will be written to the text file `baseline_losses_rank[factors_rank].txt`.

To run the ALS model with rating normalization (our method) run:
```
spark-submit --class ca.uwaterloo.cs651project.MovieLensZScoreALS target/project-1.0.jar --size [small/large] --runs [no_of_runs]
```
where the `--size`, `--rank` and `--runs` arguments are as before. The test MSE for each run of the ALS algorithm with rating normalization 
will be written to the text file `normalization_losses_rank[factors_rank].txt`.

### Additional notes:
- There is no need to download the dataset. When specifying the `--size` argument, the code will automatically download the 
appropriate dataset and move it to HDFS (if it is not already there). This is taken care of by the scripts 
`download_data.sh` and `move_data_to_hdfs.sh`.
- You may need to increase your stack size to avoid a `StackOverflowError`. To do this locally, run
`ulimit -s unlimited`. When running on the cluster, add `--conf "spark.executor.extraJavaOptions=-Xss10000000m"` argument in the `spark-submit` command
to ensure that the stack size of the worker nodes is big enough for the code to run without causing a `StackOverflowError`.


