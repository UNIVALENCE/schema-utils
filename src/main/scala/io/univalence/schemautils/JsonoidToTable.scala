package io.univalence.schemautils

import org.apache.spark.sql.types.{ArrayType, DataType, NullType, StructType}
import org.apache.spark.sql.{Column, DataFrame}

case class AtomicFieldPath private (names: Vector[String] = Vector.empty, dataType: DataType) {
  def add(name: String, dataType: DataType): AtomicFieldPath = AtomicFieldPath(names :+ name, dataType)
  def add(name: String): AtomicFieldPath = add(name, dataType)
}

object AtomicFieldPath {
  val empty = AtomicFieldPath(Vector.empty, NullType)
}

object JsonoidToTable {

  def allDirectlyAccessibleFields(structType: StructType, prefix: AtomicFieldPath): Seq[AtomicFieldPath] = {
    for {
      field <- structType.fields
      fieldpath <- field.dataType match {
        case subStruct: StructType => allDirectlyAccessibleFields(subStruct, prefix.add(field.name))
        case _:         ArrayType  => Nil
        case dt => List(prefix.add(field.name, dt))
      }
    } yield fieldpath
  }

  def apply(df: DataFrame): DataFrame = {
    val schema: StructType = df.schema

    //expr parses a sql expression like 'case a.b.c when 2 then 2 + 1 else d end as xyz' into a column
    import org.apache.spark.sql.functions.expr

    val cols: Seq[Column] = for {
      fieldPath <- allDirectlyAccessibleFields(schema, AtomicFieldPath.empty)
    } yield {
      val fieldref: Column = expr(fieldPath.names.mkString("."))
      val name:     String = fieldPath.names.mkString("_")

      fieldref.as(name)
    }

    df.select(cols: _*)
  }
}
