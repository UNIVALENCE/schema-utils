package io.univalence.schemautils

import java.io.PrintWriter

import org.apache.spark.sql.types.{ArrayType, DataType, StructType}
import org.apache.spark.sql.{DataFrame, SaveMode}
import org.scalatest.FunSuite

import scala.io.Source
import scala.util.Try

class ModeleHTest extends FunSuite with SparkTest {

  lazy val loadModeleH: DataFrame = ss.read
    .option("basePath", "file:///Users/jon/GDrive/UNIVALENCE/Data/For Jon From Yacine")
    .option("mergeSchema", "true")
    .parquet("/Users/jon/GDrive/UNIVALENCE/Data/For Jon From Yacine/*/*/*.parquet")

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

  ignore("reproduce bug") {
    dfFromJson({ "" })

  }

  def sizePath(path: Path): Int = {
    path.fold[Int](1, (_, names, opt) => names.size + 1 + opt, _ + 2)
  }


  object TxModelh {

    import FlattenNestedTargeted._

    type Endo = DataFrame => DataFrame

    import org.apache.spark.sql.functions._

    val detachVisite: Endo = in => {
      val cols: Array[String] = "explode(visites) as visite" +: in.columns.filter(_ != "visites")

      val res1 = in.select(cols.map(expr): _*)

      val visiteSchema = res1.schema.fields.find(_.name == "visite").get.dataType.asInstanceOf[StructType]

      val colsRoot = res1.schema.fields.filter(_.name != "visite").map(_.name) ++ visiteSchema.map(x =>
        s"visite.${x.name} as ${x.name}")

      res1.select(colsRoot.map(expr): _*)
    }

    val detachLrs: Endo = in =>
      detach(
        dataFrame   = in,
        target      = Path.select.recherches.>.history.>.bandeaux.>.lrs,
        fieldname   = _.mkString("_"),
        includeRoot = x => None,
        outer       = false
      )

    val detachHistory: Endo = in =>
      detach(
        dataFrame   = in,
        target      = Path.select.recherches.>.history,
        fieldname   = _.mkString("_"),
        includeRoot = x => Some(x.mkString("_")),
        outer       = true
      )

    val toRemoveHistory: Seq[String] =
      Seq("numerocompte", "url", "produitsdisplay", "isfirst", "idrequete", "isvalidatedbyatinternet","previouspageurl")


    val moveHistoryFields: Endo = {
      transformAtPath(
        Path.select.history.>,
        (dt, root) => {
          val structType             = dt.asInstanceOf[StructType]
          val allCols: Array[String] = structType.fields.map(_.name)

          val (colsToMove, colsToKeep) = allCols.partition(x => toRemoveHistory.contains(x.toLowerCase))

          val lrsType =
            structType.fields.find(_.name == "lrs")
              .get.dataType.asInstanceOf[ArrayType]
              .elementType.asInstanceOf[StructType]

          val toKeepWithOutLRS = colsToKeep.filterNot(_ == "lrs").map(name => {
            (SingleExp(s"$root.$name"),name)
          })

          val lrRoot = "lrRoot"
          val struct = StructExp(colsToMove.map(name => SingleExp(s"$root.$name") -> name) ++  lrsType.fieldNames.map(name => SingleExp(s"$lrRoot.$name") -> name))

          StructExp(
            toKeepWithOutLRS :+ (SingleExp(s"transform($root.lrs, $lrRoot -> ${struct.exp})") -> "lrs")
          )
        }
      )
    }

    val dropRecherches:Endo = dropField(Path.select.recherches)

    val renameHistory:Endo = renameField(Path.select.history,"recherches")



    val aggregateNumerocompte:Endo = addFieldAtPath(Path.select.recherches.>,{
      case (dt,root) => ( SingleExp(s"element_at($root.lrs,1).numerocompte") , "numerocompte")
    })


    val tx:Endo  = Seq[Endo](
      detachVisite,
      detachLrs,
      detachHistory,
      moveHistoryFields,
      dropRecherches,
      renameHistory,
      dropField(Path.select.recherches.>.lrs.>.nbreponses),
      aggregateNumerocompte,
      dropField(Path.select.recherches.>.lrs.>.numerocompte)
    ).reduce(_ andThen _)


  }

  test("remove history2") {

    loadModeleH.printSchema()

    TxModelh.tx(loadModeleH).write.mode(SaveMode.Overwrite).partitionBy("datasource","no_event").parquet("target/outOneDay")


  }

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

    val out = tx(loadModeleH)

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

    val drop = FlattenNestedTargeted
      .allPaths(out.schema)
      .flatMap(select)
      .distinct
      .map(dropField)
      .reduce(_ andThen _)

    //out.printSchema()

    //out.explain(true)

    drop(out).write.mode(SaveMode.Overwrite).parquet("target/outOneDay")

  }

  ignore("blank") {

    loadModeleH.write.mode(SaveMode.Overwrite).parquet("target/testOut")
  }

  ignore("rdd") {

    loadModeleH.sparkSession
      .createDataFrame(loadModeleH.rdd, loadModeleH.schema)
      .write
      .mode(SaveMode.Overwrite)
      .parquet("target/testOut")
  }

  ignore("toto") {
    val schema = fastLoadSchema()

    schema.printTreeString()

    //visites
    //history
    //bandeaux
    FlattenNestedTargeted
      .allPaths(schema)
      .filter(x => sizePath(x) >= 15) //+ 3 + 3 + 3)
      .foreach(x => println(sizePath(x) -> x))

  }

  test("show") {

    println(ss.read.parquet("target/outOneDay").count())
  }

}
