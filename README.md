## Personalized PageRank with ALS

Test

### Requirements:
The Java, Scala and Spark versions used here are the same as the ones 
used in the course 'CS651: Data-Intensive Distributed Computing'. Namely:
- Java: 1.8.0
- Scala: 2.11.8
- Spark: 2.3.1

To build the project, `sbt` is required. The version used here is 1.10.1 
but any reasonably recent version should work.

### How to run:
To build the project, first do:
```
sbt update
```
To install spark. Then run:
```
sbt clean assembly
```
To run it, use:
```
 spark-submit --class ca.uwaterloo.cs651project.MovieLens target/scala-2.11/PersonalizedPagerankALS-assembly-0.1.0.jar --size [small/large] 
```
The `--size` argument is optional. If not provided, the small dataset will be used.
To run it locally, you can instead use the simpler command `sbt run` (it will automatically use the small dataset).
