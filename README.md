## Personalized PageRank with ALS

TODO: Add description

### Requirements:
The Java, Scala and Spark versions used here are the same as the ones 
used in the course 'CS651: Data-Intensive Distributed Computing'. Namely:
- Java: 1.8.0
- Scala: 2.11.8
- Spark: 2.3.1

To build the project, `sbt` is required. The version used here is 1.10.1 
but any reasonably recent version should work.

### How to run:
To build the project, run:
```
sbt clean compile package
```
To run it, use:
```
spark-submit --class ca.uwaterloo.cs651project.Main target/scala-2.11/personalizedpagerankals_2.11-0.1.0.jar 
```
To run it locally, you can instead use the simpler command `sbt run`.
