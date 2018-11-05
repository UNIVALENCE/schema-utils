package io.univalence.schemautils
import org.apache.spark.sql.SparkSession

object TestSparkSession {
  val ss: SparkSession = SparkSession.builder().master("local").getOrCreate()
}
