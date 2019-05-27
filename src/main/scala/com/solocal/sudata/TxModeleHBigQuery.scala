package com.solocal.sudata

import io.univalence.schemautils.FlattenNestedTargeted.{addFieldAtPath, detach, dropField, renameField, transformAtPath}
import io.univalence.schemautils.{FlattenNestedTargeted, Path, SingleExp, StructExp}
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.apache.spark.sql.types.{ArrayType, StructType}

object TxModelh {

  import FlattenNestedTargeted._

  type Endo = DataFrame => DataFrame

  import org.apache.spark.sql.functions._

  val detachVisite: Endo = in => {

    //TODO refactor to use detach
    val cols: Array[String] = "explode(visites) as visite" +: in.columns.filter(_ != "visites")

    val res1 = in.select(cols.map(expr): _*)

    val visiteSchema = res1.schema.fields.find(_.name == "visite").get.dataType.asInstanceOf[StructType]

    val colsRoot = res1.schema.fields.filter(_.name != "visite").map(_.name) ++ visiteSchema.map(x =>
      s"visite.${x.name} as ${x.name}")

    res1.select(colsRoot.map(expr): _*)
  }

  val detachLrs: Endo = in =>
    detach(
      dataFrame = in,
      //TODO use typedpath
      // path"recherches/history/bandeaux/lrs"
      target      = Path.select.recherches.>.history.>.bandeaux.>.lrs,
      fieldname   = _.mkString("_"),
      includeRoot = x => None,
      outer       = false
  )

  val detachHistory: Endo = in =>
    detach(
      dataFrame = in,
      //path"recherches/history"
      target      = Path.select.recherches.>.history,
      fieldname   = _.mkString("_"),
      includeRoot = x => Some(x.mkString("_")),
      outer       = true
  )

  val moveHistoryFields: Endo = {
    val toRemoveHistory: Seq[String] =
      Seq("numerocompte",
          "url",
          "produitsdisplay",
          "isfirst",
          "idrequete",
          "isvalidatedbyatinternet",
          "previouspageurl")

    transformAtPath(
      Path.select.history.>,
      (dt, root) => {
        val structType             = dt.asInstanceOf[StructType]
        val allCols: Array[String] = structType.fields.map(_.name)

        val (colsToMove, colsToKeep) = allCols.partition(x => toRemoveHistory.contains(x.toLowerCase))

        val lrsType =
          structType.fields
            .find(_.name == "lrs")
            .get
            .dataType
            .asInstanceOf[ArrayType]
            .elementType
            .asInstanceOf[StructType]

        val toKeepWithOutLRS = colsToKeep
          .filterNot(_ == "lrs")
          .map(name => {
            (SingleExp(s"$root.$name"), name)
          })

        val lrRoot = "lrRoot"
        val struct =
          StructExp(colsToMove.map(name => SingleExp(s"$root.$name") -> name) ++ lrsType.fieldNames.map(name =>
            SingleExp(s"$lrRoot.$name") -> name))

        StructExp(
          toKeepWithOutLRS :+ (SingleExp(s"transform($root.lrs, $lrRoot -> ${struct.exp})") -> "lrs")
        )
      }
    )
  }

  val dropRecherches: Endo = dropField(Path.select.recherches)

  val renameHistory: Endo = renameField(Path.select.history, "recherches")

  val aggregateNumerocompte: Endo = addFieldAtPath(Path.select.recherches.>, {
    case (dt, root) => (SingleExp(s"element_at($root.lrs,1).numerocompte"), "numerocompte")
  })

  val tx: Endo = Seq[Endo](
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

  def apply(dataFrame: DataFrame): DataFrame = tx(dataFrame)
}

object TxModeleHBigQuery {

  def main(args: Array[String]): Unit = {
    args match {
      case Array(baseDir, outDir) =>
        val ss = SparkSession.builder().appName(this.getClass.getName).master("local[8]").getOrCreate()
        val loadModeleH = ss.read
          .option("basePath", baseDir)
          .option("mergeSchema", "true")
          .parquet(s"$baseDir/*/*/*.parquet")

        loadModeleH.printSchema()

        val out = TxModelh(loadModeleH)
        println("--------- new Schema ------------")
        out.printSchema()

        out
          .write
          //.mode(SaveMode.Overwrite)
          .partitionBy("datasource", "no_event")
          .parquet(outDir)

      case _ => println(s"""
           |Yo!
           |
          |call the program with $$sourceBaseDir and $$outBaseDir
           |got ${args.toList}
           |
        """.stripMargin)

    }

  }

}
