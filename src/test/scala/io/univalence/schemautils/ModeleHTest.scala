package io.univalence.schemautils

import java.io.PrintWriter

import io.univalence.schemautils.FlattenNestedTargeted.{Path, PathPart}
import org.apache.spark.sql.{DataFrame, SaveMode}
import org.apache.spark.sql.types.{DataType, StructType}
import org.scalatest.FunSuite

import scala.io.Source
import scala.util.Try

class ModeleHTest extends FunSuite with SparkTest {

  lazy val loadModeleH: DataFrame = ss.read
    .parquet("/Users/jon/Downloads/part-00000-b72d11c6-55d0-4e30-a1e7-f58cacced654-c000.snappy.parquet")

  def loadSchema: StructType = {
    loadModeleH.schema

  }

  def saveSchema(structType: StructType): Unit = {
    new PrintWriter("schema.json") { write(structType.prettyJson); close() }
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

  test("reproduce bug") {
    dfFromJson({ "" })

  }

  test("remove history") {

    type Endo = DataFrame => DataFrame

    val detachProduitsDisplay: Endo = in =>
      FlattenNestedTargeted.detach(
        dataFrame   = in,
        target      = Path.fromString("visites.[].recherches.[].history.[].produitsDisplay"),
        fieldname   = _.mkString("_"),
        includeRoot = x => Some(("history" :: x.toList).mkString("_")),
        outer       = false
    )

    val suggestion: Endo = in =>
      FlattenNestedTargeted.detach(
        dataFrame   = in,
        target      = Path.fromString("visites.[].recherches.[].history.[].suggestion"),
        fieldname   = _.mkString("_"),
        includeRoot = x => Some(("history" :: x.toList).mkString("_")),
        outer       = false
    )

    val detachLrs1: Endo = in =>
      FlattenNestedTargeted.detach(
        dataFrame   = in,
        target      = Path.fromString("visites.[].recherches.[].history.[].bandeaux.[].lrs"),
        fieldname   = _.mkString("_"),
        includeRoot = x => Some(("bandeau" :: x.toList).mkString("_")),
        outer       = false
    )

    val detachLrs2: Endo = in =>
      FlattenNestedTargeted.detach(
        dataFrame   = in,
        target      = Path.fromString("visites.[].recherches.[].history.[].lrs"),
        fieldname   = _.mkString("_"),
        includeRoot = x => Some(("history" :: x.toList).mkString("_")),
        outer       = false
    )

    val detachRecherches: Endo = in =>
      FlattenNestedTargeted.detach(
        dataFrame   = in,
        target      = Path.fromString("visites.[].recherches"),
        fieldname   = _.mkString("_"),
        includeRoot = x => Some(("visite" :: x.toList).mkString("_")),
        outer       = false
    )

    val tx = Seq(
      detachRecherches
      //detachLrs1,detachLrs2,detachProduitsDisplay,suggestion,

      //,suggestion,detachLrs1,detachLrs2,detachRecherches
    ).reduce((f, g) => f.andThen(g))

    val out = tx(loadModeleH)

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
      .allPaths(out.schema)
      .filter(x => sizePath(x) >= 15) //+ 3 + 3 + 3)
      .foreach(x => println(sizePath(x) -> x))

    out.limit(10).write.mode(SaveMode.Overwrite).parquet("target/testOut")

  }

  def dropField(path: Path, df: DataFrame): DataFrame = {

    val PathPart.Field(name) = path.last

    FlattenNestedTargeted.transformAtPath(path.init, {
      case (st: StructType, expr) => StructExp(st.fieldNames.filter(_ != name).map(x => SingleExp(s"$expr.$x") -> x))
    })(df)
  }

  test("toto") {
    val schema = fastLoadSchema()

    schema.printTreeString()

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
      .filter(x => sizePath(x) >= 15) //+ 3 + 3 + 3)
      .foreach(x => println(sizePath(x) -> x))

  }

}
