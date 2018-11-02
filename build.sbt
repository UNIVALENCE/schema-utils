scalaVersion := "2.11.12"

libraryDependencies += "org.apache.spark" %% "spark-sql" % "2.3.2" % Provided
libraryDependencies += "org.scalatest"    %% "scalatest" % "3.0.5" % Test

scalafmtOnCompile := true
