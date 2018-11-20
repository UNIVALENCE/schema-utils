package io.univalence.schemautils

import io.univalence.schemautils.FlattenNestedTargeted.Path
import org.scalatest.FunSpecLike

class TransformAtPathTest extends FunSpecLike with SparkTest {

  describe("Transform at path of a nested JSON document") {
    val df = dfFromJson("""
             { a: 1,
               b: [{ c: [{ d: 4 },
                         { d: 5 }],
                     e: 6 },
                   { f: 7 }]
             }""")

    it("increase a value in the root") {
      val result = FlattenNestedTargeted.transformAtPath(Path.fromString("a"), (_, x) => SingleExp(s"$x + 1"))(df)

      assertDfEqual(
        result,
        dfFromJson("""
             { a: 2,
               b: [{ c: [{ d: 4 },
                         { d: 5 }],
                     e: 6 },
                   { f: 7 }]
             }""")
      )
    }

    it("should increase a value nested in a struct and an array") {
      val result = FlattenNestedTargeted.transformAtPath(Path.fromString("b.[].e"), (_, x) => SingleExp(s"$x + 1"))(df)

      assertDfEqual(
        result,
        dfFromJson("""
             { a: 1,
               b: [{ c: [{ d: 4 },
                         { d: 5 }],
                     e: 7 },
                   { f: 7 }]
             }""")
      )
    }

    it("should increase a value in nested structs") {
      val in     = dfFromJson("{a:{b:{c:3,d:4}}}")
      val result = FlattenNestedTargeted.transformAtPath(Path.fromString("a.b.c"), (_, x) => SingleExp(s"$x + 1"))(in)

      assertDfEqual(
        result,
        dfFromJson("{a:{b:{c:4,d:4}}}")
      )
    }
  }

}
