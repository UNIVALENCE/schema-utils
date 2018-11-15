package io.univalence.schemautils
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types.{ArrayType, DataType, StructField, StructType}

import scala.annotation.tailrec
import scala.util.{Random, Try}

object FlattenNestedTargeted {

  case class Tablename(name: String) extends AnyVal

  def sql(input: DataFrame)(query: Tablename => String): DataFrame = {
    val ss        = input.sparkSession
    val inputName = "input" + Random.nextInt(10000)
    input.createTempView(inputName)
    val out = ss.sql(query(Tablename(inputName)))
    ss.catalog.dropTempView(inputName)
    out
  }

  def alignDataframe(df: DataFrame, schema: StructType): DataFrame = {
    def genSelect(dataType: DataType, exp: String, top: Boolean = false): String = {
      dataType match {
        case StructType(fields) =>
          val allFields: Seq[String] = fields.map(x => genSelect(x.dataType, exp + "." + x.name) + " as " + x.name)
          if (top) allFields.mkString(", ") else allFields.mkString("struct(", ",", ")")

        case ArrayType(elementType, _) =>
          s"transform($exp, x -> ${genSelect(elementType, "x")})"
        case _ => exp
      }
    }

    sql(df)(x => s"select ${genSelect(schema, x.name, top = true)} from ${x.name}")
  }

  sealed trait PathPart
  object PathPart {
    case class Field(name: String) extends PathPart {
      override def toString: String = name
    }
    case object Array extends PathPart {
      override def toString: String = "[]"
    }
  }

  type Path = Seq[PathPart]

  object Path {
    def fromString(str: String): Path = {
      str
        .split('.')
        .map({
          case "[]" => PathPart.Array
          case x    => PathPart.Field(x)
        })
    }

    def toString(path: Path): String = path.mkString(".")
  }

  def allPaths(dataType: DataType): Seq[Path] = {
    dataType match {
      case StructType(fields) => fields.flatMap(x => allPaths(x.dataType).map(y => PathPart.Field(x.name) +: y)).toSeq
      case ArrayType(e, _)    => allPaths(e).map(PathPart.Array +: _)
      case _                  => Seq(Nil)
    }
  }
  @tailrec
  def dataTypeAtPath(target: Path, dataType: DataType): Try[DataType] = {
    (target, dataType) match {
      case (Seq(), x)                                   => Try(x)
      case (Seq(PathPart.Array, xs @ _*), s: ArrayType) => dataTypeAtPath(xs, s.elementType)
      case (Seq(PathPart.Field(name), xs @ _*), s: StructType) =>
        dataTypeAtPath(xs, s.fields(s.fieldIndex(name)).dataType)
      case _ => Try(???)
    }
  }

  def txPath(target: Path, tx: (DataType, String) => String)(dataFrame: DataFrame): DataFrame = {

    def rewrite(dataType: DataType, target: Path, top: Boolean = false, expr: String): String = {
      (target, dataType) match {
        case (Seq(), _) => tx(dataType, expr)
        case (Seq(PathPart.Array, xs @ _*), ArrayType(elementType, _)) =>
          s"transform($expr, x -> ${rewrite(elementType, xs, top = false, "x")})"

        case (Seq(PathPart.Field(name), xs @ _*), StructType(fields)) =>
          val exprs = fields.map({
            case StructField(`name`, dt, _, _) => rewrite(dt, xs, expr = expr + "." + name) + s" as $name"
            case StructField(x, dt, _, _)      => s"$expr.$x as $x"
          })

          if (top) exprs.mkString(", ") else exprs.mkString("struct(", ", ", s")")
      }

    }

    val str = rewrite(dataFrame.schema, target, top = true, "toto")

    dataFrame.createTempView("toto")

    val x = dataFrame.sparkSession.sql(s"select $str from toto")
    dataFrame.sparkSession.catalog.dropTempView("toto")
    x
  }

  def detach(dataFrame: DataFrame,
             target: Path,
             fieldname: Seq[String]   => String,
             includeRoot: Seq[String] => Option[String],
             addLink: Boolean = true,
             outer: Boolean   = true): DataFrame = {

    val (scope, follow) = target.splitAt(target.lastIndexOf(PathPart.Array) - 1)

    val Seq(PathPart.Field(init), PathPart.Array, rest @ _*) = follow

    txPath(
      scope,
      (dt, s) => {

        val fields = dt.asInstanceOf[StructType].fields

        val x: StructType =
          fields.find(_.name == init).get.dataType.asInstanceOf[ArrayType].elementType.asInstanceOf[StructType]

        val all = x.fieldNames.filter(_ != rest.head.asInstanceOf[PathPart.Field].name)

        val xs = all.flatMap(name => includeRoot(name :: Nil).map(n => s"x.$name as $n")).mkString(",")

        val name = fieldname(rest.map({ case PathPart.Field(n) => n }))

        val y = dataTypeAtPath(rest, x).get.asInstanceOf[ArrayType].elementType.asInstanceOf[StructType]

        val ys = y.fieldNames.map(n => s"y.$n as $n").mkString(",")

        val root_xs = all.map(name => s"x.$name as $name").mkString(", ")

        val root = dt
          .asInstanceOf[StructType]
          .fieldNames
          .map({
            case `init` =>
              s"""
             transform($s.$init, x -> struct($root_xs)) as $init
          """

            case n => s"$s.$n as $n"
          })

        val in = rest.mkString(".")


        val empty_ys = y.fields.map(f => s"cast(null as ${f.dataType.catalogString}) as ${f.name}").mkString(", ")


        val outer_array = s"array(struct($xs, $empty_ys))"


        val transform_x = s"""transform(x.$in, y -> struct($xs, $ys))"""


        val txs = if(outer)
         s"""flatten(transform($s.$init,
            x -> if(cardinality(x.$in) >= 0,
            $transform_x,
            $outer_array
            )


            )) as $name
          """
        else {
          s"""flatten(
           transform(filter($s.$init, x -> cardinality(x.$in) >= 0),
             x -> $transform_x )) as $name"""

        }

        val zs   = (root :+ txs).mkString(",")
        val res2 = s"""struct($zs)"""

        res2

      }
    )(dataFrame)

  }

  def apply(dataframe: DataFrame, target: Path, prefix: String): Try[DataFrame] = {
    val ss = dataframe.sparkSession

    ???
  }

}
