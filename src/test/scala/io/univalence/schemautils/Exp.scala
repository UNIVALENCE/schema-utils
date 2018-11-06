package io.univalence.schemautils
import org.apache.spark.sql.types.StructType

object Exp {

  import SchemaDsl._

  val schema: StructType = struct(firstname = string,
                                  lastname = string,
                                  contact  = struct(`type` = string, value = struct(dept = string, id = string)))

  def main(args: Array[String]): Unit = {

    schema.printTreeString()
  }
}
