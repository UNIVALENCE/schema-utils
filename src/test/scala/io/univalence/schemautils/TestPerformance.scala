package io.univalence.schemautils

import java.util.UUID

import org.apache.hadoop.util.hash.MurmurHash
import org.apache.spark.sql.SaveMode

import scala.util.{Random, Try}
import scala.util.hashing.MurmurHash3

case class Effect[T](run: () => Try[T])

object Effect {
  def syncException[T](exp: => T): Effect[T] = Effect(() => Try(exp))
}

case class CC(id: String,
              i1: Int,
              l2: Long,
              s3: String,
              e4: String,
              d5: String,
              t6: Long,
              a7: Seq[Int],
              a8: Seq[CC2] = Nil)

case class CC2(id: String, i1: Int, l2: Long, s3: String, e4: String, d5: String, t6: Long, a7: Seq[Int])

object TestPerformance {

  def longToSeed(lng: java.lang.Long): Int = MurmurHash3.bytesHash(Array(lng.byteValue()))

  def genClass2(seed: Int): CC2 = {
    val random = new Random(seed)
    CC2(
      id = random.alphanumeric.take(16).mkString,
      i1 = random.nextInt(),
      l2 = random.nextLong(),
      s3 = random.alphanumeric.take(random.nextInt(200)).mkString,
      e4 = longToSeed(random.nextLong()).toString,
      d5 = "",
      t6 = random.nextLong(),
      a7 = (0 to random.nextInt(1024)).map(_ => random.nextInt())
    )
  }

  def genClass(seed: Int): CC = {
    val random = new Random(seed)

    // a more FP way ? // it is somewhat deterministic
    CC(
      id = random.alphanumeric.take(16).mkString,
      i1 = random.nextInt(),
      l2 = random.nextLong(),
      s3 = random.alphanumeric.take(random.nextInt(200)).mkString,
      e4 = longToSeed(random.nextLong()).toString,
      d5 = "",
      t6 = random.nextLong(),
      a7 = (0 to random.nextInt(1024)).map(_ => random.nextInt()),
      a8 = (0 to random.nextInt(24)).map(_ => genClass2(random.nextInt()))
    )
  }

  def generateSample: Effect[Unit] = {
    Effect.syncException({

      import SparkTest.ss.implicits._

      SparkTest.ss
        .range(1000000)
        .repartition(48)
        .map(longToSeed)
        .map(genClass)
        .write
        .mode(SaveMode.Overwrite)
        .option("compression", "snappy")
        .parquet("target/sample/randomNested")
    })
  }

  def main(args: Array[String]): Unit = {

    generateSample.run()

    System.exit(0)

    val ss = SparkTest.ss

    val taskMetrics = ch.cern.sparkmeasure.TaskMetrics(ss)

    taskMetrics.runAndMeasure {
      ss.sql("select count(*) from range(100000)").show()
    }

    taskMetrics.runAndMeasure({

      import ss.implicits._
      val ds = ss.range(100000).map(x => x)

      ds.registerTempTable("xxx")

      ss.sql("select count(*) from xxx").show()
    })
    // generate data dataset, generate summary of it
    // show diff in performance

    //-prof gc

  }

}
