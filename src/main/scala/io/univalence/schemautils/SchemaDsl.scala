package io.univalence.schemautils

import org.apache.spark.sql.types._

import scala.language.dynamics

object SchemaDsl {

  val string: StringType.type = StringType

  object struct extends Dynamic {
    def applyDynamicNamed(name: String)(args: (String, DataType)*): StructType = {
      StructType(args.map({ case (n, d) => StructField(n, d) }))
    }
  }

  def array(dataType: DataType): ArrayType = ArrayType(dataType)

}
