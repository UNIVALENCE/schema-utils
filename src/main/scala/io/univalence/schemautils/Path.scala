package io.univalence.schemautils

import io.univalence.schemautils.Path.{NonEmptyPath, SelectField}

import scala.language.{dynamics, implicitConversions}

sealed trait Path {

  final def asCode: String = Path.unfold(this).map(_.getOrElse(">")).mkString(".")

  def select: SelectField = {
    this match {
      case Path.Empty      => Path.SelectOnEmpty
      case x: NonEmptyPath => Path.Select(x)
    }
  }

  def fold[B](empty: B, field: (String, Seq[String], B) => B, array: B => B): B = {
    this match {
      case f: Path.Field => field(f.name, f.names, f.parent.fold(empty, field, array))
      case a: Path.Array => array(a.parent.fold(empty, field, array))
      case Path.Empty    => empty
    }
  }

}

object Path {

  def select: SelectField = SelectOnEmpty

  def fromString(str: String): Path = {
    str
      .split('.')
      .toList match {
      case Nil => Path.Empty
      case x :: xs =>
        xs.foldLeft[NonEmptyPath](Path.select.field(x))({
          case (p, ">") => p.select.>
          case (p, n)   => p.select.field(n)
        })

    }
  }

  case class Field(name: String, protected[Path] val names: Seq[String], parent: FieldParent) extends NonEmptyPath {
    def allNames: Seq[String] = name +: names

    def this(name: String) = { this(name, Nil, Empty) }

    def directParent: (Path, String) = {
      if (names.isEmpty)
        parent -> name
      else
        this.copy(names = names.init) -> names.last
    }
  }

  case class Array(parent: NonEmptyPath) extends NonEmptyPath with FieldParent with Path
  case object Empty                      extends FieldParent with Path

  implicit def toPath1[T <: NonEmptyPath](select: Select[T]): T = select.path

  type UnfoldedPath = Seq[Option[String]]

  def unfold(path: Path): UnfoldedPath = {
    path match {
      case Path.Empty => Nil
      case f: Path.Field =>
        unfold(f.parent) ++ f.allNames.map(Some.apply)
      case Path.Array(parent) => unfold(parent) :+ None
    }
  }

  sealed trait FieldParent extends Path

  sealed trait NonEmptyPath extends Path {
    override def select = Path.Select(this)
  }

  sealed trait SelectField extends Dynamic {

    protected def fieldImp(name: String): Select[Path.Field]

    private def checkName(name: String): String = {
      name match {
        case "[]" => throw new Exception("not supported, use '>'")
        case ">"  => throw new Exception("use an NonEmptyPath")
        case _    => name
      }
    }

    final def field(name: String): Select[Path.Field]         = fieldImp(checkName(name))
    final def selectDynamic(name: String): Select[Path.Field] = field(name)
  }

  case object SelectOnEmpty extends SelectField {
    def fieldImp(name: String): Select[Path.Field] = Select(new Path.Field(name))
  }

  case class Select[+T <: NonEmptyPath](path: T) extends SelectField {
    def fieldImp(name: String): Select[Path.Field] = {
      path match {
        case f: Path.Field   => Select(f.copy(names = f.names :+ name))
        case arr: Path.Array => Select(Path.Field(name, Nil, arr))
      }
    }

    def `>` : Select[Path.Array] = Select(Path.Array(path))
  }
}
