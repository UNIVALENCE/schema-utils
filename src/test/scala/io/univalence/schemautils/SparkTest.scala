package io.univalence.schemautils

import org.apache.spark.sql.{DataFrame, Dataset, Encoder, SparkSession}
import org.scalatest.{Assertions, FunSuite}

import scala.reflect.ClassTag

trait SparkTest extends Assertions {

  val ss: SparkSession =
    SparkSession
      .builder()
      .master("local[2]")
      .config("spark.default.parallelism", 1)
      .config("spark.ui.enabled",true)
      .getOrCreate()

  def smallDs[T: Encoder: ClassTag](t: T*): Dataset[T] = {
    import ss.implicits._

    ss.sparkContext.parallelize(t, 1).toDS
  }

  def dfFromJson(jsonStr: String*): DataFrame = {
    import ss.implicits._

    ss.read
      .option("allowUnquotedFieldNames", value = true)
      .json(smallDs(jsonStr: _*))
  }

  def assertDfEqual(f1: DataFrame, f2: DataFrame): Unit =
    assert(f1.toJSON.collect().toList == AlignDataframe(f2, f1.schema).toJSON.collect().toList)

  def assertDsEqual[A](ds: Dataset[A], as: A*): Unit = {
    assert(ds.collect().toSeq == as)

  }

}

object SparkTest extends SparkTest
