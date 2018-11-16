package io.univalence.schemautils

import java.io.PrintWriter

import io.univalence.schemautils.FlattenNestedTargeted.{Path, PathPart}
import org.apache.spark.sql.types.{DataType, StructType}
import org.scalatest.FunSuite

import scala.io.Source
import scala.util.Try

class ModeleHTest extends FunSuite {

  def loadSchema: StructType = {
    val df = TestSparkSession.ss.read
      .parquet("/Users/jon/Downloads/part-00000-b72d11c6-55d0-4e30-a1e7-f58cacced654-c000.snappy.parquet")
    df.schema

  }

  def saveSchema(structType: StructType): Unit = {
    new PrintWriter("schema.json") { write(structType.prettyJson); close }
  }

  def loadSchemaFromFile(): Try[StructType] = {
    Try {
      DataType.fromJson(Source.fromFile("schema.json").mkString).asInstanceOf[StructType]
    }
  }

  def fastLoadSchema(): StructType = {
    loadSchemaFromFile().getOrElse({

      val s = loadSchema
      saveSchema(s)
      s
    })

  }

  test("toto") {
    val schema = fastLoadSchema()

    def sizePath(path: Path): Int = {

      1 + path
        .map({
          case PathPart.Array => 2
          case _              => 1
        })
        .sum

    }

    //visites
    //history
    //bandeaux
    FlattenNestedTargeted
      .allPaths(schema)
      .filter(x => sizePath(x) >= 15 + 3 + 3 + 3)
      .foreach(x => println(sizePath(x) -> x))

  }

}
