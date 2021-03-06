package io.univalence.schemautils

import java.io.PrintWriter

import org.apache.spark.sql.types.{ArrayType, DataType, StructType}
import org.apache.spark.sql.{DataFrame, SaveMode}
import org.scalatest.FunSuite

import scala.io.Source
import scala.util.Try

class ModeleHTest extends FunSuite with SparkTest {

  def saveSchema(structType: StructType): Unit = {
    new PrintWriter("schema.json") { write(structType.prettyJson); close() }
  }

  def loadSchemaFromFile(): Try[StructType] = {
    Try {
      DataType.fromJson(Source.fromFile("schema.json").mkString).asInstanceOf[StructType]
    }
  }

  def fastLoadSchema: StructType = loadSchemaFromFile().get

  ignore("reproduce bug") {
    dfFromJson({ "" })

  }

  def sizePath(path: Path): Int = {
    path.fold[Int](1, (_, names, opt) => names.size + 1 + opt, _ + 2)
  }

  ignore("remove history2") {}

  ignore("remove history") {

    import FlattenNestedTargeted._

    type Endo = DataFrame => DataFrame

    val detachProduitsDisplay: Endo = in =>
      detach(
        dataFrame   = in,
        target      = Path.select.visites.>.recherches.>.history.>.produitsDisplay,
        fieldname   = _.mkString("_"),
        includeRoot = x => Some(("history" :: x.toList).mkString("_")),
        outer       = false
    )

    val suggestion: Endo = in =>
      detach(
        dataFrame   = in,
        target      = Path.select.visites.>.recherches.>.history.>.suggestion,
        fieldname   = _.mkString("_"),
        includeRoot = x => Some(("history" +: x).mkString("_")),
        outer       = false
    )

    val detachLrs1: Endo = in =>
      detach(
        dataFrame   = in,
        target      = Path.select.visites.>.recherches.>.history.>.bandeaux.>.lrs,
        fieldname   = _.mkString("_"),
        includeRoot = x => Some(("bandeau" +: x).mkString("_")),
        outer       = false
    )

    val detachLrs2: Endo = in =>
      detach(
        dataFrame   = in,
        target      = Path.select.visites.>.recherches.>.history.>.lrs,
        fieldname   = _.mkString("_"),
        includeRoot = x => Some(("history" +: x).mkString("_")),
        outer       = false
    )

    val detachRecherches: Endo = in =>
      detach(
        dataFrame   = in,
        target      = Path.select.visites.>.recherches,
        fieldname   = _.mkString("_"),
        includeRoot = x => Some(("visite" +: x).mkString("_")),
        outer       = false
    )

    //val dropLrsHistoryProduitsDisplay:Endo = in => dropField(Path.fromString("recherches.[].lrs.[].history_produitsDisplay"),in)

    val tx = Seq(
      detachLrs1,
      detachLrs2,
      detachProduitsDisplay,
      suggestion,
      detachRecherches

      //,suggestion,detachLrs1,detachLrs2,detachRecherches
    ).reduce((f, g) => f.andThen(g))

    //val out = tx(loadModeleH)

    //visites
    //history
    //bandeaux

    val toRemove: Set[String] = Set("history_bandeaux",
                                    "visite_pagesAT",
                                    "history_lrs_link",
                                    "history_produitsDisplay_link",
                                    "history_produitsDisplay")

    def select(path: Path): Option[Path.Field] = {
      path match {
        case Path.Empty         => None
        case Path.Array(parent) => select(parent)
        case f: Path.Field =>
          val (path, name) = f.directParent
          if (toRemove(name)) Some(f)
          else select(path)
      }
    }

    /*
    val drop = FlattenNestedTargeted
      .allPaths(out.schema)
      .flatMap(select)
      .distinct
      .map(dropField)
      .reduce(_ andThen _)

    //out.printSchema()

    //out.explain(true)

    drop(out).write.mode(SaveMode.Overwrite).parquet("target/outOneDay")
   */

  }

  ignore("blank") {

    //loadModeleH.write.mode(SaveMode.Overwrite).parquet("target/testOut")
  }

  ignore("rdd") {}

  ignore("toto") {
    val schema = fastLoadSchema

    schema.printTreeString()

    //visites
    //history
    //bandeaux
    FlattenNestedTargeted
      .allPaths(schema)
      .filter(x => sizePath(x) >= 15) //+ 3 + 3 + 3)
      .foreach(x => println(sizePath(x) -> x))

  }

}
