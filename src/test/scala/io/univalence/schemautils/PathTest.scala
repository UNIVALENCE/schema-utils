package io.univalence.schemautils
import io.univalence.schemautils
import org.scalatest.FunSuite

class PathTest extends FunSuite {

  test("testFromString") {

    assert(Path.fromString("xxx.visites") == Path.Field("xxx", Seq("visites"), Left(Path.Empty)))

    val a = Path.Array(new schemautils.Path.Field("a"))

    assert(Path.select.a.>.path == a)
    assert(Path.fromString("a.[]") == a)

    assert(Path.fromString("xxx.visites.>.recherches").asCode == "xxx.visites.>.recherches")

  }

}
