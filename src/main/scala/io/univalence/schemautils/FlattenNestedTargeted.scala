package io.univalence.schemautils

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.types.{ArrayType, DataType, StructField, StructType}

import scala.annotation.tailrec
import scala.util.{Random, Try}

//uStateMonad
case class State[S, A](run: S => (A, S)) {
  def map[B](f: A => B): State[S, B] =
    State(s => {
      val (a, n) = run(s)
      (f(a), n)
    })

  def flatMap[B](f: A => State[S, B]): State[S, B] =
    State(s => {
      val (a, n) = this.run(s)
      f(a).run(n)
    })

  def exec(state: S): A = run(state)._1
}

object GenSym {
  val nextSym: State[Int, String] = State[Int, String](x => ("tmpSym" + x, x + 1))

  val genTempTableName_! : SparkSession => String = {
    var x = 0

    def f(ss: SparkSession): String = {
      x = x + 1
      val name = "tempTbl" + x
      if (ss.catalog.tableExists(name)) f(ss) else name
    }
    f
  }
}

object FlattenNestedTargeted {

  def offset(seq: Seq[Int]): Seq[Option[Seq[Int]]] = {
    var offset = 1

    seq.map(x => {
      val start = offset
      val size  = Math.max(x, 0)
      offset += size
      start.until(start + size)

      val y: Option[Seq[Int]] =
        if (x == -1) None
        else Some(start.until(start + size))

      y
    })
  }

  def offset_outer(seq: Seq[Int]): Seq[Option[Seq[Int]]] = {
    var offset = 1

    seq.map(x => {
      val start = offset
      val size  = Math.max(x, 1)
      offset += size

      val y: Option[Seq[Int]] =
        if (x == -1) None
        else if (x == 0) Some(Nil)
        else Some(start.until(start + size))

      y
    })
  }

  case class Tablename(name: String) extends AnyVal

  def sql(input: DataFrame)(query: Tablename => String): DataFrame = {
    val ss: SparkSession = input.sparkSession

    val tempTableName: String = GenSym.genTempTableName_!(ss)

    input.createTempView(tempTableName)

    val out: DataFrame = ss.sql(query(Tablename(tempTableName)))

    ss.catalog.dropTempView(tempTableName)

    out
  }

  def alignDataframe(df: DataFrame, schema: StructType): DataFrame = {

    sealed trait Out {
      def exp: String
    }
    case class SingleExp(exp: String) extends Out

    case class StructExp(fieldExps: Seq[(Out, String)]) extends Out {
      override def exp: String = fieldExps.map(x => x._1.exp + " as " + x._2).mkString("struct(", ", ", ")")
      def asProjection: String = fieldExps.map(x => x._1.exp + " as " + x._2).mkString(", ")
    }

    def genSelectStruct(structType: StructType, source: String): StructExp = {
      def genSelectDataType(dataType: DataType, source: String): Out =
        dataType match {
          case st: StructType => genSelectStruct(st, source)

          case ArrayType(elementType, _) =>
            SingleExp(s"transform($source, x -> ${genSelectDataType(elementType, "x").exp})")

          case _ => SingleExp(source)
        }

      StructExp(structType.fields.map(x => genSelectDataType(x.dataType, source + "." + x.name) -> x.name))
    }

    sql(df)(tmpTableName =>
      s"select ${genSelectStruct(schema, tmpTableName.name).asProjection} from ${tmpTableName.name}")
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

    def fromString(str: String): Path =
      str
        .split('.')
        .map({
          case "[]" => PathPart.Array
          case x    => PathPart.Field(x)
        })

    def toString(path: Path): String = path.mkString(".")

  }

  def allPaths(dataType: DataType): Seq[Path] =
    dataType match {
      case StructType(fields) => fields.flatMap(x => allPaths(x.dataType).map(y => PathPart.Field(x.name) +: y)).toSeq
      case ArrayType(e, _)    => allPaths(e).map(PathPart.Array +: _)
      case _                  => Seq(Nil)
    }

  @tailrec
  def dataTypeAtPath(target: Path, dataType: DataType): Try[DataType] =
    (target, dataType) match {
      case (Seq(), x)                                   => Try(x)
      case (Seq(PathPart.Array, xs @ _*), s: ArrayType) => dataTypeAtPath(xs, s.elementType)
      case (Seq(PathPart.Field(name), xs @ _*), s: StructType) =>
        dataTypeAtPath(xs, s.fields(s.fieldIndex(name)).dataType)
      case _ => Try(???)
    }

  def transformAtPath(target: Path, tx: (DataType, String) => String)(dataFrame: DataFrame): DataFrame = {

    def rewrite(dataType: DataType, target: Path, top: Boolean = false, expr: String): String =
      (target, dataType) match {
        case (Seq(), _) => tx(dataType, expr)
        case (Seq(PathPart.Array, xs @ _*), ArrayType(elementType, _)) =>
          s"transform($expr, x -> ${rewrite(elementType, xs, top = false, "x")})"

        case (Seq(PathPart.Field(name), xs @ _*), StructType(fields)) =>
          val exprs = fields.map({
            case StructField(`name`, dt, _, _) => rewrite(dt, xs, expr = expr + "." + name) + s" as $name"
            case StructField(x, dt, _, _)      => s"$expr.$x as $x"
          })

          if (top) exprs.mkString(", ")
          else exprs.mkString("struct(", ", ", s")")
      }

    val tempTableName = GenSym.genTempTableName_!(dataFrame.sparkSession)
    dataFrame.createTempView(tempTableName)

    val projection = rewrite(dataFrame.schema, target, top = true, tempTableName)
    val out        = dataFrame.sparkSession.sql(s"select $projection from $tempTableName")

    dataFrame.sparkSession.catalog.dropTempView(tempTableName)

    out
  }

  def detach(dataFrame: DataFrame,
             target: Path,
             fieldname: Seq[String] => String,
             includeRoot: Seq[String] => Option[String],
             addLink: Boolean = true,
             outer: Boolean   = true): DataFrame = {
    dataFrame.sparkSession.udf.register("offset_outer", offset_outer _)
    dataFrame.sparkSession.udf.register("offset", offset _)

    val (scope, follow) = target.splitAt(target.lastIndexOf(PathPart.Array) - 1)

    val Seq(PathPart.Field(init), PathPart.Array, rest @ _*) = follow

    transformAtPath(
      scope,
      (dt, s) => {

        val fields = dt.asInstanceOf[StructType].fields

        val x: StructType =
          fields.find(_.name == init).get.dataType.asInstanceOf[ArrayType].elementType.asInstanceOf[StructType]

        val all = x.fieldNames.filter(_ != rest.head.asInstanceOf[PathPart.Field].name)

        val xs = all.flatMap(name => includeRoot(name :: Nil).map(n => s"x.$name as $n")).mkString(",")

        val name: String = fieldname(rest.map({ case PathPart.Field(n) => n }))

        val y = dataTypeAtPath(rest, x).get.asInstanceOf[ArrayType].elementType.asInstanceOf[StructType]

        val ys = y.fieldNames.map(n => s"y.$n as $n").mkString(",")

        val root_xs = all.map(name => s"x.$name as $name").mkString(", ")

        val in = rest.mkString(".")

        val root: Array[String] =
          dt.asInstanceOf[StructType]
            .fieldNames
            .map({
              case `init` =>
                if (addLink) {
                  val size_array: String =
                    if (outer)
                      s"""offset_outer(transform($s.$init, x -> cardinality(x.$in)))"""
                    else
                      s"""offset(transform($s.$init, x -> cardinality(x.$in)))"""

                  s"zip_with($size_array,$s.$init, (n,x) -> struct($root_xs, n as ${name}_link )) as $init"
                } else
                  s"""transform($s.$init, x -> struct($root_xs)) as $init"""
              case n => s"$s.$n as $n"
            })

        val empty_ys = y.fields.map(f => s"cast(null as ${f.dataType.catalogString}) as ${f.name}").mkString(", ")

        val outer_array = s"array(struct($xs, $empty_ys))"

        val transform_x = s"""transform(x.$in, y -> struct($xs, $ys))"""

        val txs =
          if (outer)
            s"""flatten(transform($s.$init,
            x -> if(cardinality(x.$in) > 0,
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
