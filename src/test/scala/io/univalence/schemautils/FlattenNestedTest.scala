package io.univalence.schemautils
import io.univalence.schemautils.TestSparkSession.ss
import org.apache.spark.sql.DataFrame
import org.scalatest.FunSuite

class FlattenNestedTest extends FunSuite {


  test("basics") {
    val documentA: String = """
                    {
                      "a":{
                        "b":1,
                        "c":2,
                        "d":{
                          "e":3
                        }
                      },
                      "h": [{"i":9},{"i":10,"j":[{"k":11}]}]
                    }"""

    import ss.implicits._
    val in0: DataFrame = ss.read.json(ss.createDataset(Seq(documentA)))

    import org.apache.spark.sql.functions.expr

    val df1 = in0.select(expr("a"),expr("explode_outer(h)").as("h"))
    val df2 = df1.select(expr("a"),expr("h.i as h_i"),expr("explode_outer(h.j) as h_j"))
    val df3 = df2.select(expr("a"),expr("if(h_i is not null or h_j is not null, struct(h_i as i,h_j as j), null) as h"))


    assert(FlattenNested.contract(in0.schema) == df3.schema)


    val df4 = FlattenNested(in0)
    in0.printSchema()
    df4.printSchema()

    df4.show(false)
    //assert(df4.schema == df3.schema)
  }

}
