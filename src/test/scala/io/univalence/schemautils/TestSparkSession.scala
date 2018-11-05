package io.univalence.schemautils
import org.apache.spark.sql.SparkSession

object TestSparkSession {
  val ss: SparkSession = SparkSession
    .builder()
    .master("local")
    .config("spark.default.parallelism", 1)
    .config("spark.ui.enabled", false)
    .getOrCreate()
}
