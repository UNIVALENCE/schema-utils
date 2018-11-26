package io.univalence.schemautils

import io.univalence.schemautils.Path.{Part, _impl}

import scala.language.dynamics

sealed trait Path {

  def prefix(name: String): Path

  def asCode: String

  def inArray: Path

  def dsl: Path.Dsl

  def parts: Seq[Part]

  sealed trait Split {
    def last: Option[(Path, Path.Part)]
    def head: Option[(Path.Part, Path)]
    def lastArray: Option[(Path, Path)]

  }

  def split: Split

}

object Path {

  sealed trait Dsl extends Dynamic {

    def `>` : Path with Dsl

    def selectDynamic(name: String): Path with Dsl
  }

  private case class _impl(parts: Seq[Part]) extends Path with Dsl {
    override def asCode: String                             = parts.map({ case Part.Array => ">"; case Part.Field(name) => name }).mkString(".")
    override def `>` : Path with Dsl                        = _impl(parts :+ Part.Array)
    override def selectDynamic(name: String): Path with Dsl = _impl(parts :+ Part.Field(name))
    override def prefix(name: String): Path                 = _impl(Part.Field(name) +: parts)
    override def inArray: Path                              = _impl(Part.Array +: parts)
    override def dsl: Dsl                                   = this

    object split extends Split {
      def last: Option[(Path, Path.Part)] =
        if (parts.nonEmpty) {
          Some(_impl(parts.init) -> parts.last)
        } else {
          None
        }

      def head: Option[(Path.Part, Path)] = {
        if (parts.nonEmpty)
          Some(parts.head -> _impl(parts.tail))
        else
          None
      }
      def lastArray: Option[(Path, Path)] =
        parts.lastIndexOf(Path.Part.Array) match {
          case -1 => None
          case n =>
            val (x, y) = parts.splitAt(n)
            Some(_impl(x) -> _impl(y.drop(1)))
        }
    }
  }

  sealed trait Part

  final object Part {
    final case class Field(name: String) extends Part
    final case object Array              extends Part
  }

  val root: Path with Dsl = _impl(Vector.empty)

  def fromString(str: String): Path = {
    _impl(str.split('.').map({ case ">" | "[]" => Part.Array; case n => Part.Field(n) }))
  }

  def fromParts(parts: Seq[Part]): Path = _impl(parts)

}

object SmallApp {

  def main(args: Array[String]): Unit = {

    Path.root.a.>.b

    println(Path.fromString("a.>.b").asCode)

  }

}
