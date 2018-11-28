package io.univalence.schemautils

import scala.language.{dynamics, implicitConversions}

case class Select[+T <: Path](path: T) extends Dynamic {
  def field(name: String): Select[Path.Field] = {
    path match {
      case f: Path.Field   => Select(f.copy(names = f.names :+ name))
      case arr: Path.Array => Select(Path.Field(name, Nil, Right(arr)))
      case _               => Select(new Path.Field(name))
    }
  }

  def selectDynamic(name: String): Select[Path.Field] = {
    field(name)
  }

  def `>` : Select[Path.Array] = {
    path match {
      case x: NonEmptyPath => Select[Path.Array](Path.Array(x))
      case _               => ???
    }
  }
}

/*
case class Prefix(path: Path) {
  def inField(name: String): Path = ???
  def inArray: Path               = ???
}
 */

sealed trait Path {

//  def prefix: Prefix = Prefix(this)

  def asCode: String
  def select: Select[Path] = Select(this)

}

object Path {

  def select = Select(Path.Empty)

  def fromString(str: String): Path = {
    str
      .split("\\.")
      .foldLeft[Path](Path.Empty)((p, s) =>
        s match {
          case "[]" | ">" => p.select.>
          case _          => p.select.field(s)
      })
  }

  case class Field(name: String, names: Seq[String], parent: Either[Empty.type, Array]) extends NonEmptyPath {
    override def asCode: String = parent.fold(_ => "", _.asCode + ".") + (name +: names).mkString(".")

    def this(name: String) = { this(name, Nil, Left(Empty)) }

    def directParent: (Path, String) = {
      if (names.isEmpty)
        parent.fold[Path](x => x, x => x) -> name
      else
        this.copy(names = names.init) -> names.last

    }
  }

  case class Array(parent: NonEmptyPath) extends NonEmptyPath {
    override def asCode: String = parent.asCode + ".>"
  }

  case object Empty extends Path {
    override def asCode: String = ""
  }

  implicit def toPath1[T <: Path](select: Select[T]): T = select.path

  type UnfoldedPath = Seq[Option[String]]

  def unfold(path: Path): UnfoldedPath = {
    path match {
      case Path.Empty => Nil
      case Path.Field(name, names, parent) =>
        unfold(parent.fold[Path](identity, identity)) ++ (name +: names).map(Some.apply)
      case Path.Array(parent) => unfold(parent) :+ None
    }
  }
}

sealed trait NonEmptyPath extends Path {

  def fold[B](field: (String, Seq[String], Option[B]) => B, array: B => B): B = {
    this match {
      case f: Path.Field => field(f.name, f.names, f.parent.fold(_ => None, x => Some(x.fold(field, array))))
      case a: Path.Array => array(a.parent.fold(field, array))
    }
  }

}
