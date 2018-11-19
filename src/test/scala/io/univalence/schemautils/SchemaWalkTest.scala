package io.univalence.schemautils

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types._
import org.scalatest.{FunSuiteLike, Matchers}

class SchemaWalkTest extends FunSuiteLike with Matchers with SparkTest {

  import ss.implicits._

  test("should have same data type when same type") {
    SchemaWalk.sameDatatype(IntegerType, IntegerType) should be(true)
  }

  test("should not have same data type when different types") {
    SchemaWalk.sameDatatype(IntegerType, BooleanType) should be(false)
  }

  test("should have same data type for arrays with same nested type") {
    SchemaWalk.sameDatatype(ArrayType(IntegerType), ArrayType(IntegerType)) should be(true)
  }

  test("should have same data type for structure with same nested type and same field name") {
    SchemaWalk.sameDatatype(StructType(StructField("a", IntegerType) :: Nil),
                            StructType(StructField("a", IntegerType) :: Nil)) should be(true)
  }

  test("should not have same data type for structure with same nested type and different field names") {
    SchemaWalk.sameDatatype(StructType(StructField("a", IntegerType) :: Nil),
                            StructType(StructField("b", IntegerType) :: Nil)) should be(false)
  }

  test("should not have same data type for structure with different nested types and same field name") {
    SchemaWalk.sameDatatype(StructType(StructField("a", IntegerType) :: Nil),
                            StructType(StructField("a", BooleanType) :: Nil)) should be(false)
  }

  test("should validate dataframe when its schema matches the contract") {
    val df: DataFrame =
      Seq(
        ("1", Array(1, 2))
      ).toDF("id", "values")

    (SchemaWalk.validateSchema(df,
                               StructType(
                                 StructField("id", StringType)
                                   :: StructField("values", ArrayType(IntegerType))
                                   :: Nil))
      should be(Some(df)))
  }

  test("should not validate dataframe when its schema does not match the contract") {
    val df: DataFrame =
      Seq(
        ("1", Array(1, 2))
      ).toDF("id", "values")

    (SchemaWalk.validateSchema(df,
                               StructType(
                                 StructField("id", StringType)
                                   :: StructField("values", ArrayType(BooleanType))
                                   :: Nil))
      should be(None))
  }

}
