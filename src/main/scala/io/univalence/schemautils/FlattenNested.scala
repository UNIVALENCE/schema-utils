package io.univalence.schemautils

import io.univalence.schemautils.SchemaWalk.fold
import org.apache.spark.sql.types.{DataType, StringType, StructField, StructType}
import org.apache.spark.sql.{Column, DataFrame}

import scala.collection.immutable

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

    def unrold(dataFrame: DataFrame,
               dive: Either[Field, Explode],
               notYetExploded: List[Explode],
               atomicSym: Seq[String]): DataFrame = {
      val in: Seq[SelectOrExplode] = dive.fold(x => selectOrExplode(x), y => selectOrExplode(y.element, Vector(y.sym)))

      val toSelect: Seq[Select]    = in.collect({ case x: Select => x })
      val toExplode: List[Explode] = notYetExploded ++ in.collect({ case x: Explode => x }).toList

      val toSelectCols = toSelect.map({ case Select(path, sym) => pathToCol(path).as(sym) })
      val atomicCols   = atomicSym.map(dataFrame.col)

      toExplode match {
        case Nil => dataFrame.select(toSelectCols ++ atomicCols: _*)
        case x :: xs =>
          val gen: Column = org.apache.spark.sql.functions.explode_outer(pathToCol(x.path)).as(x.sym)

          val explosion: immutable.Seq[Column] = gen :: xs.map(x => pathToCol(x.path).as(x.sym))

          val newDf = dataFrame.select(toSelectCols ++ atomicCols ++ explosion: _*)

          unrold(newDf, Right(x), xs.map(x => x.copy(path = Vector(x.sym))), atomicSym ++ toSelect.map(_.sym))
      }

    }

    val flatten = unrold(dataFrame, Left(schemaWithSym), Nil, Nil)

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
