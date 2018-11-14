package io.univalence.schemautils
import org.apache.spark.sql.{DataFrame, Dataset, Encoder, SparkSession}

import scala.reflect.ClassTag

object TestSparkSession {
  val ss: SparkSession = SparkSession
    .builder()
    .master("local")
    .config("spark.default.parallelism", 1)
    .config("spark.ui.enabled", false)
    .getOrCreate()

  def smallDs[T: Encoder: ClassTag](t: T*): Dataset[T] = {
    import ss.implicits._
    ss.sparkContext.parallelize(t, 1).toDS
  }

  def dfFromJson(jsonStr: String*): DataFrame = {
    import ss.implicits._
    ss.read.option("allowUnquotedFieldNames", true).json(smallDs(jsonStr: _*))
  }
}
