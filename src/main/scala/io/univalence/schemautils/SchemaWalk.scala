package io.univalence.schemautils

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types.{ArrayType, DataType, StructType}

object SchemaWalk {

  def fold[B](array: B => B, struct: Seq[(String, B)] => B, atomic: DataType => B)(dataType: DataType): B = {
    val recur: DataType => B = fold(array, struct, atomic)

    dataType match {
      case ArrayType(elementType, _) => array(recur(elementType))
      case StructType(fields)        => struct(fields.map(x => x.name -> recur(x.dataType)))
      case _                         => atomic(dataType)
    }
  }

  type FieldPath = List[String]

  def allDirectlyAccessibleFields(dataType: DataType): Seq[FieldPath] =
    fold[Seq[FieldPath]](array = _ => Nil,
                         struct = fields =>
                           for {
                             (name, fieldPaths) <- fields
                             fieldPath          <- fieldPaths
                           } yield name :: fieldPath,
                         atomic = _ => Vector(Nil))(dataType)

  def sameDatatype(d1: DataType, d2: DataType): Boolean = {
    (d1, d2) match {
      case (ArrayType(e1, _), ArrayType(e2, _)) => sameDatatype(e1, e2)
      case (StructType(f1), StructType(f2)) =>
        f1.zip(f2).forall({ case (sf1, sf2) => sf1.name == sf2.name && sameDatatype(sf1.dataType, sf2.dataType) })
      case (x, y) => x == y
    }
  }

  def validateSchema(dataframe: DataFrame, contract: StructType): Option[DataFrame] =
    if (sameDatatype(dataframe.schema, contract)) Option(dataframe)
    else None
}
