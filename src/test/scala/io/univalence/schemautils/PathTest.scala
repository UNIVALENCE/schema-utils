package io.univalence.schemautils
import io.univalence.schemautils
import org.scalatest.FunSuite

class PathTest extends FunSuite {

  test("testFromString") {

    assert(Path.fromString("xxx.visites") == Path.Field("xxx", Seq("visites"), Path.Empty))

    val a = Path.Array(new schemautils.Path.Field("a"))

    assert(Path.select.a.>.path == a)
    assert(Path.fromString("a.>") == a)

    assert(Path.fromString("xxx.visites.>.recherches").asCode == "xxx.visites.>.recherches")

  }
  test("string context") {
    import Path._
    val path: Path = path"xxx.visites/recherches"

    assert(path == Field("recherches", Nil, Array(Field("xxx", "visites" :: Nil, Empty))))
    assert(path.asCode == "xxx.visites/recherches")
  }

  test("more test") {
    import Path._
    assert(path"" == Path.Empty)
    assert(path"abc/" == Path.Array(Path.Field("abc",Nil,Empty)))
  }
}
