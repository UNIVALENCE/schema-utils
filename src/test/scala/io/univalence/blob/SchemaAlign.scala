package io.univalence.blob
import io.univalence.schemautils.{FlattenNestedTargeted, SparkTest}
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
import org.apache.spark.sql.{Dataset, Encoder}
import org.scalatest.FunSuite

case class A(b: String, c: Long)

case class E(as: Seq[A], f: String)

class SchemaAlign extends FunSuite with SparkTest {

  test("blog article") {

    import ss.implicits._

    val ds1: Dataset[A] = smallDs(A("1", 2), A("3", 4))

    ds1.show(false)

    val ds2: Dataset[A] = dfFromJson("{b:'5',c:6, d:7}", "{b:'8',c:9, d:10}").as[A]

    ds2.show(false)

    //Union can only be performed on tables with the same number of columns, but the first table has 2 columns and the second table has 3 columns;;
    //assertDsEqual(ds1.union(ds2), A("1", 2), A("3", 4), A("5", 6), A("8", 9))

    //may work
    {
      val ds3: Dataset[A] = ds2.drop("d").as[A]
      assertDsEqual(ds1.union(ds3), A("1", 2), A("3", 4), A("5", 6), A("8", 9))
    }

    //won't work
    {
      val ds3: Dataset[A] = ds2.select("c", "b").as[A]
      //Cannot up cast `c` from string to bigint as it may truncate
      //assertDsEqual(ds1.union(ds3), A("1", 2), A("3", 4), A("5", 6), A("8", 9))
    }

    //always work si les datasets ont le même nombre de champ
    {
      val ds3: Dataset[A] = ds2.drop("d").as[A]
      assertDsEqual(ds1.unionByName(ds3), A("1", 2), A("3", 4), A("5", 6), A("8", 9))

    }

    def betterDatasetUnion0[A](ds1: Dataset[A], ds2: Dataset[A]): Dataset[A] = {
      implicit val exprEnc: Encoder[A] = {
        val field = classOf[Dataset[A]].getDeclaredField("exprEnc")
        field.setAccessible(true)
        field.get(ds1).asInstanceOf[ExpressionEncoder[A]]

        //au lieu de ds1.exprEnc
      }

      val name :: names = exprEnc.schema.fieldNames.toList
      ds1.select(name, names: _*).union(ds2.select(name, names: _*)).as[A]
    }

    def betterDatasetUnion[A: Encoder](ds: Dataset[A], dss: Dataset[A]*): Dataset[A] = {
      val fields: Array[String] = implicitly[Encoder[A]].schema.fieldNames
      (ds +: dss).map(x => x.select(fields.map(x.apply): _*)).reduce(_ union _).as[A]
    }

    //always work, period !
    {
      betterDatasetUnion0(ds1, ds2)
      val res: Dataset[A] = betterDatasetUnion(ds1, ds2, ds2)
      assertDsEqual(res, A("1", 2), A("3", 4), A("5", 6), A("8", 9), A("5", 6), A("8", 9))
    }

    //until you use complex structure

    {
      val e1              = E(Seq(A("1", 2)), "3")
      val ds1: Dataset[E] = smallDs[E](e1)
      val ds2: Dataset[E] = dfFromJson("{as:[{b:'5',c:6, d:7}, {b:'8',c:9, d:10}],f:'11'}").as[E]

      //Union can only be performed on tables with the compatible column types. array<struct<b:string,c:bigint,d:bigint>> <> array<struct<b:string,c:bigint>> at the first column of the second table;;
      //val res: Dataset[E] = betterDatasetUnion(ds1, ds2)
      //assertDsEqual(res, e1, E(Seq(A("5",6),A("8",9)),"11"))

    }

    def betterDatasetUnion2[A: Encoder](ds: Dataset[A], dss: Dataset[A]*): Dataset[A] = {
      val schema = implicitly[Encoder[A]].schema
      import io.univalence.schemautils.AlignDataframe
      (ds +: dss).map(x => AlignDataframe(x.toDF(), schema)).reduce(_ union _).as[A]
    }

    {
      val e1              = E(Seq(A("1", 2)), "3")
      val ds1: Dataset[E] = smallDs[E](e1)
      val ds2: Dataset[E] = dfFromJson("{as:[{b:'5',c:6, d:7}, {b:'8',c:9, d:10}],f:'11'}").as[E]

      val res: Dataset[E] = betterDatasetUnion2(ds1, ds2)
      assertDsEqual(res, e1, E(Seq(A("5", 6), A("8", 9)), "11"))

    }

  }

  test("slow") {
    Thread.sleep(100000)
  }

}
