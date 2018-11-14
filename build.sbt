scalaVersion := "2.11.12"

libraryDependencies += "org.apache.spark" %% "spark-sql" % "2.4.0" % Provided

libraryDependencies += "org.apache.spark" %% "spark-streaming" % "2.4.0" % Provided

libraryDependencies += "org.scalatest" %% "scalatest" % "3.0.5" % Test

scalafmtOnCompile := true

javaOptions ++= Seq("-Xms512M", "-Xmx2048M", "-XX:+CMSClassUnloadingEnabled")

testOptions in Test += Tests.Argument(TestFrameworks.ScalaTest, "-oD")
