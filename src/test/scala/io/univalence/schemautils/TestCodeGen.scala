package io.univalence.schemautils

object TestCodeGen {

  import io.univalence.schemautils.SparkTest
  import org.apache.spark.sql.{Column, DataFrame, Dataset, SparkSession}



  val ss =  SparkSession.builder()
    .master("local")
    .appName("toto")
    .config("spark.ui.enabled", false)
    .getOrCreate()



  import org.apache.spark.sql.functions._

  val df20 = SparkTest.dfFromJson("{a:3, b: 4}")
  val res = df20.select(expr("a + b  + 42 as c"), df20("b"))

  import org.apache.spark.sql.execution.debug._

  res
  //res.debugCodegen()


  def main(args: Array[String]): Unit = {
    res.debugCodegen()
  }
}
