ThisBuild / version := "0.1.0"

ThisBuild / scalaVersion := "2.11.8"

lazy val sparkVersion = "2.4.8"

libraryDependencies ++= Seq(
	"org.apache.spark" %% "spark-core" % sparkVersion,
	"org.apache.spark" %% "spark-sql" % sparkVersion,
	"org.apache.spark" %% "spark-graphx" % sparkVersion,
	"org.rogach" % "scallop_2.11" % "3.0.1"
)

resolvers += Resolver.mavenCentral

lazy val root = (project in file("."))
  .settings(
    name := "PersonalizedPagerankALS",
    idePackagePrefix := Some("ca.uwaterloo.cs651project")
  )
