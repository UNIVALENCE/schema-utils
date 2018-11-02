package io.univalence.schemautils

import org.apache.spark.sql.types.{ArrayType, DataType, NullType, StructType}
import org.apache.spark.sql.{Column, DataFrame}

case class AtomicFieldPath(names: Vector[String], dataType: DataType)

object JsonoidToTable {

  def allDirectlyAccessibleFields(structType: StructType): Seq[AtomicFieldPath] = {

    def innerLoop(structType: StructType, prefix: Vector[String]): Seq[AtomicFieldPath] = {
      for {
        field <- structType.fields
        fieldpath <- field.dataType match {
          case subStruct: StructType => innerLoop(subStruct, prefix :+ field.name)
          case _: ArrayType          => Nil
          case dt                    => List(AtomicFieldPath(prefix :+ field.name, dt))
        }
      } yield fieldpath
    }

    innerLoop(structType, Vector.empty)
  }

  def apply(df: DataFrame): DataFrame = {
    val schema: StructType = df.schema

    //expr parses a sql expression like 'case a.b.c when 2 then 2 + 1 else d end as xyz' into a column
    import org.apache.spark.sql.functions.expr

    val cols: Seq[Column] = for {
      fieldPath <- allDirectlyAccessibleFields(schema)
    } yield {
      val fieldref: Column = expr(fieldPath.names.mkString("."))
      val name: String     = fieldPath.names.mkString("_")

      fieldref.as(name)
    }

    df.select(cols: _*)
  }
}
