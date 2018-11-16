package io.univalence.schemautils

import org.apache.spark.sql.DataFrame
import org.scalatest.FunSuiteLike

class FlattenNestedTest extends FunSuiteLike with TestSparkSession {

  test("basics") {
    val documentA: String = """
                    {
                      "a":{
                        "b":1,
                        "c":2,
                        "d":{
                          "e": 3
                        }
                      },
                      "h": [{"i":9},{"i":10,"j":[{"k":11}]}]
                    }"""

    val in0: DataFrame = TestSparkSession.dfFromJson(documentA)

    import org.apache.spark.sql.functions.expr

    //méthode manual

    val df1 = in0.select(expr("a"), expr("explode_outer(h)").as("h"))
    val df2 = df1.select(expr("a"), expr("h.i as h_i"), expr("explode_outer(h.j) as h_j"))
    val df3 =
      df2.select(expr("a"), expr("if(h_i is not null or h_j is not null, struct(h_i as i,h_j as j), null) as h"))

    //assert(FlattenNested.contract(in0.schema) == df3.schema)

    //méthode automatik
    val df4 = FlattenNested(in0)

    val doc1 = """{"a":{"b":1,"c":2,"d":{"e":3}},"h":{"i":9,"j":{}}}"""
    val doc2 = """{"a":{"b":1,"c":2,"d":{"e":3}},"h":{"i":10,"j":{"k":11}}}"""

    assert(df4.toJSON.collect() sameElements Array(doc1, doc2))
  }

  test("double array") {
    val document: String = """{"a":[1,2],"b":[3,4]}"""

    val in0: DataFrame = TestSparkSession.dfFromJson(document)

    val df = FlattenNested(in0)

    val doc1 = """{"a":1,"b":3}"""
    val doc2 = """{"a":1,"b":4}"""
    val doc3 = """{"a":2,"b":3}"""
    val doc4 = """{"a":2,"b":4}"""

    assert(df.toJSON.collect() sameElements Array(doc1, doc2, doc3, doc4))
  }

}
