package io.univalence.schemautils

import org.apache.spark.sql.types.{ArrayType, DataType, NullType, StructType}
import org.apache.spark.sql.{Column, DataFrame}

case class AtomicFieldPath private (names: Vector[String] = Vector.empty, dataType: DataType) {
  def add(name: String, dataType: DataType): AtomicFieldPath = AtomicFieldPath(names :+ name, dataType)
  def add(name: String): AtomicFieldPath                     = add(name, dataType)
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
        case _: ArrayType          => Nil
        case dt                    => Seq(prefix.add(field.name, dt))
      }
    } yield fieldpath
  }

  def apply(df: DataFrame): DataFrame = {

    val schema = df.schema

    import org.apache.spark.sql.functions._

    val cols: Seq[Column] = allDirectlyAccessibleFields(schema, AtomicFieldPath.empty) map { fieldPath =>
      //val fieldref:Column = fieldPath.names.tail.foldLeft(df(fieldPath.names.head))(_ getField _)

      val fieldref: Column = expr(fieldPath.names.mkString("."))
      val name: String     = fieldPath.names.mkString("_")

      fieldref.as(name)
    }

    df.select(cols: _*)

  }
}
