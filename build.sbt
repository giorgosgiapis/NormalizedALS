ThisBuild / version := "0.1.0"

ThisBuild / scalaVersion := "2.11.8"

lazy val root = (project in file("."))
  .settings(
    name := "PersonalizedPagerankALS",
    idePackagePrefix := Some("ca.uwaterloo.cs651project")
  )
