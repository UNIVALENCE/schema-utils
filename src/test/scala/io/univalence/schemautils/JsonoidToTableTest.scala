package io.univalence.schemautils

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types._
import org.scalatest.FunSuiteLike
import scala.language.dynamics

class JsonoidToTableTest extends FunSuiteLike with SparkTest {

  ignore("story") {

    val documentA = """
                    {
                      "a":{
                        "b":1,
                        "c":2,
                        "d":{
                          "e":3
                        }
                      },
                      "f":4,
                      "g": [5,6,7,8],
                      "h": [{"i":9},{"i":10}]
                    }
                    """

    val in: DataFrame = dfFromJson(documentA)

    in.printSchema()
    /*
    root
     |-- a: struct (nullable = true)
     |    |-- b: long (nullable = true)
     |    |-- c: long (nullable = true)
     |    |-- d: struct (nullable = true)
     |    |    |-- e: long (nullable = true)
     |-- f: long (nullable = true)
     |-- g: array (nullable = true)
     |    |-- element: long (containsNull = true)
     |-- h: array (nullable = true)
     |    |-- element: struct (containsNull = true)
     |    |    |-- i: long (nullable = true)
     */

    in.show(false)
    /*
    +-----------+---+------------+-----------+
    |a          |f  |g           |h          |
    +-----------+---+------------+-----------+
    |[1, 2, [3]]|4  |[5, 6, 7, 8]|[[9], [10]]|
    +-----------+---+------------+-----------+
     */

    //in.write.csv("target/data/out.csv")
    /*
    CSV data source does not support struct<b:bigint,c:bigint,d:struct<e:bigint>> data type.
    java.lang.UnsupportedOperationException: CSV data source does not support struct<b:bigint,c:bigint,d:struct<e:bigint>> data type.
     */

    val ds = JsonoidToTable(in)

    val l = ds.queryExecution.logical

    ds.show(false)
    /*
    +---+---+-----+---+
    |a_b|a_c|a_d_e|f  |
    +---+---+-----+---+
    |1  |2  |3    |4  |
    +---+---+-----+---+
   */
  }

  test("in and out") {
    val documentA     = """
                    {
                      "a":{
                        "b":1,
                        "c":2,
                        "d":{
                          "e":3
                        }
                      },
                      "f":4,
                      "g": [5,6,7,8],
                      "h": [{"i":9},{"i":10}]
                    }
                    """
    val in: DataFrame = dfFromJson(documentA)

    //println(in.schema.prettyJson)
    assert(JsonoidToTable(in).toJSON.head() == """{"a_b":1,"a_c":2,"a_d_e":3,"f":4}""")
  }

  test("allDirectlyAccessible") {
    import SchemaDsl._
    val schema = struct(a = struct(b = LongType, d = struct(e = LongType)), h = array(struct(i = LongType)))

    assert(
      JsonoidToTable.allDirectlyAccessibleFields(schema) == Seq(AtomicFieldPath(Vector("a", "b"), LongType),
                                                                AtomicFieldPath(Vector("a", "d", "e"), LongType)))
  }

}
