package io.univalence.schemautils
import org.apache.spark.sql.types.{ArrayType, DataType, StructType}

class SchemaWalk {

  def fold[B](array: B => B, struct: Seq[(String, B)] => B, atomic: DataType => B)(dataType: DataType): B = {
    val recur: DataType => B = fold(array, struct, atomic)
    dataType match {
      case ArrayType(elementType, _) => array(recur(elementType))
      case StructType(fields)        => struct(fields.map(x => x.name -> recur(x.dataType)))
      case _                         => atomic(dataType)
    }
  }

  type FieldPath = List[String]

  def allDirectlyAccessibleFields(dataType: DataType): Seq[FieldPath] = {
    fold[Seq[FieldPath]](array = _ => Nil,
                         struct = fields =>
                           for {
                             (name, fieldPaths) <- fields
                             fieldPath          <- fieldPaths
                           } yield name :: fieldPath ,
                         atomic = _ => Vector(Nil))(dataType)
  }

}
