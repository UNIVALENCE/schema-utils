package io.univalence.schemautils
import io.univalence.schemautils.FlattenNestedTargeted.sql
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
import org.apache.spark.sql.{DataFrame, Dataset, Encoder}
import org.apache.spark.sql.types.{ArrayType, DataType, StructType}

object AlignDataframe {

  def apply(df: DataFrame, schema: StructType): DataFrame = {

    sealed trait Out {
      def exp: String
    }
    case class SingleExp(exp: String) extends Out

    case class StructExp(fieldExps: Seq[(Out, String)]) extends Out {
      override def exp: String = fieldExps.map(x => x._1.exp + " as " + x._2).mkString("struct(", ", ", ")")
      def asProjection: String = fieldExps.map(x => x._1.exp + " as " + x._2).mkString(", ")
    }

    def genSelectStruct(structType: StructType, source: String): StructExp = {
      def genSelectDataType(dataType: DataType, source: String): Out =
        dataType match {
          case st: StructType => genSelectStruct(st, source)

          case ArrayType(elementType, _) =>
            SingleExp(s"transform($source, x -> ${genSelectDataType(elementType, "x").exp})")

          case _ => SingleExp(source)
        }

      StructExp(structType.fields.map(x => genSelectDataType(x.dataType, source + "." + x.name) -> x.name))
    }

    sql(df)(tmpTableName =>
      s"select ${genSelectStruct(schema, tmpTableName.name).asProjection} from ${tmpTableName.name}")
  }

}

object AlignDataset {

  def apply[A](dataset: Dataset[A]): Dataset[A] = {
    implicit val exprEnc: Encoder[A] = {
      val field = classOf[Dataset[A]].getDeclaredField("exprEnc")
      field.setAccessible(true)
      field.get(dataset).asInstanceOf[ExpressionEncoder[A]]
      //au lieu de ds1.exprEnc
    }

    AlignDataframe(dataset.toDF, exprEnc.schema).as[A]

  }

}
