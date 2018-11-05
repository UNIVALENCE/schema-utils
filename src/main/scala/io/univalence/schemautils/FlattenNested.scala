package io.univalence.schemautils

import io.univalence.schemautils.SchemaWalk.fold
import org.apache.spark.sql.types.{DataType, StructField, StructType}
import org.apache.spark.sql.{Column, DataFrame}

object FlattenNested {

  def contract(structType: StructType): StructType =
    SchemaWalk
      .fold[DataType](array  = element => element,
                      struct = seq     => StructType(seq.map({ case (name, dt) => StructField(name, dt) })),
                      atomic = atomic  => atomic)(structType)
      .asInstanceOf[StructType]

  def apply(dataFrame: DataFrame): DataFrame = {
    val genSym: () => String = {
      var symN = 0
      () =>
        {
          symN = symN + 1
          "col" + symN
        }
    }

    sealed trait Field {
      def sym: String
    }
    case class Atomic(sym: String)                               extends Field
    case class Array(sym: String, element: Field)                extends Field
    case class Struct(sym: String, fields: Seq[(String, Field)]) extends Field

    def field(dataType: DataType): Field =
      fold[Field](
        e      => Array(genSym(), e),
        fields => Struct(genSym(), fields),
        _      => Atomic(genSym())
      )(dataType)

    val schemaWithSym: Struct = field(dataFrame.schema).asInstanceOf[Struct]

    def pathToCol(path: Vector[String]): Column = org.apache.spark.sql.functions.expr(path.mkString("."))

    sealed trait SelectOrExplode
    case class Select(path: Vector[String], sym: String)                  extends SelectOrExplode
    case class Explode(path: Vector[String], sym: String, element: Field) extends SelectOrExplode

    def selectOrExplode(field: Field, prefix: Vector[String] = Vector.empty): Seq[SelectOrExplode] = {
      field match {
        case Atomic(sym)             => Seq(Select(prefix, sym))
        case Struct(_, fields)       => fields.flatMap({ case (n, f) => selectOrExplode(f, prefix :+ n) })
        case x @ Array(sym, element) => Seq(Explode(prefix, sym, element))
      }
    }

    def unrold(dataFrame: DataFrame, field: Option[Field], atomicSym: Seq[String], keep: Seq[Explode]): DataFrame = {
      val in: Seq[SelectOrExplode] = keep.flatMap({
        case Explode(path, sym, element) => selectOrExplode(element, Vector(sym))
      }) ++ field.map(x => selectOrExplode(x)).getOrElse(Nil)

      val newAtomicSym: Seq[String] = in.collect({ case Select(path, sym) => sym })
      val newKeep: Seq[Explode]     = in.collect({ case x: Explode        => x })

      val cols: Seq[Column] = in.map({
        case Select(path, sym)           => pathToCol(path).as(sym)
        case Explode(path, sym, element) => org.apache.spark.sql.functions.explode_outer(pathToCol(path)).as(sym)
      })

      val newDf = dataFrame.select(cols ++ atomicSym.map(dataFrame.col): _*)

      if (newKeep.isEmpty) {
        newDf
      } else {
        unrold(newDf, None, newAtomicSym ++ atomicSym, newKeep)
      }

    }

    val flatten = unrold(dataFrame, Some(schemaWithSym), Nil, Nil)

    def fieldToCol(field: Field): String = {
      field match {
        case Atomic(sym) => sym
        case Struct(sym, fields) =>
          fields.map({ case (n, f) => fieldToCol(f) + " as " + n }).mkString("struct(", ",", ")")
        case Array(sym, element) => fieldToCol(element)
      }
    }

    val res = flatten.select(schemaWithSym.fields.map({
      case (n, f) => org.apache.spark.sql.functions.expr(fieldToCol(f) + " as " + n)
    }): _*)

    SchemaWalk.validateSchema(res, contract(dataFrame.schema)).get
  }

}
