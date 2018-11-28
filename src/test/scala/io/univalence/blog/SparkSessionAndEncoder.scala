package io.univalence.blog
import org.apache.spark.sql.{DataFrame, Dataset, Encoder, SparkSession}
import org.scalatest.FunSuite

import scala.util.Try
/*

class SparkSessionAndEncoder extends FunSuite {

  ignore("Refactorer du code avec Spark") {

    /*

    Parfois on veut refactorer du code avec Spark, et on finit assez rapidement par soit avoir une SparkSession en implicit
 */

    {
      def superFonction(df: DataFrame)(implicit sparkSession: SparkSession): DataFrame = {
        ???
      }

    }
    /*

        Où on va se retrouver avec de la configuation en implicit partout
 */

    def superFonction2(df: DataFrame)(implicit sparkSession: SparkSession,
                                      config: com.typesafe.config.Config): DataFrame = {
      ???
    }

    /*
   d'un autre coté, lorsque l'on refactorise,  on se retrouve avec des erreurs d'implicit quand on rearrange le code pour reduire le niveau de duplication.
 */

    /*
    def groupByKeyRemoveNull[A, K](dataset: Dataset[A])(f: A => Option[K]): Dataset[(K, Seq[A])] = {
      dataset
        .flatMap(x => f(x).map(y => (y, x)))
        .groupByKey(_._1)
        .mapGroups({ case (k, it) => (k, it.map(_._2).toSeq) })
    }
 */

    /*
    Error:(37, 17) Unable to find encoder for type (K, A). An implicit Encoder[(K, A)] is needed to store (K, A) instances in a Dataset. Primitive types (Int, String, etc) and Product types (case classes) are supported by importing spark.implicits._  Support for serializing other types will be added in future releases.
        .flatMap(x => f(x).map(y => (y, x)))
 */

    /*

    1ère astuce : SparkSession est un membre de Dataframe/Dataset
 */
    {
      def superFonction(df: DataFrame): DataFrame = {
        val ss = df.sparkSession

        ???
      }
    }
    /*
    pas besoin de passer la sparkSession en implicit partout !



    2ème astuce : Au lieu de faire passer la configuration en implicit, faites une classe !

    Pour passer les éléments de configuration supplémentaire, on peut être tenter d'utiliser les mecanismes des implicits.


    Il peut être plus judicieux de faire une classe :

 */

    {
      object MonJob {
        def superFonction2(df: DataFrame)(implicit config: com.typesafe.config.Config): DataFrame = ???
      }

      class MonJob(config: com.typesafe.config.Config) {
        def superFonction2(df: DataFrame): DataFrame = ???
      }
    }

    /* 2bis : c'est mieux de typer ces configurations avec une hierarchie de case classes


      Les configurations Spark ou HOCON sont des formes de Json, peu typé. Pour faciliter la maintenance et rendre le runtime plus robuste, cela peut valoir le coup de passer en mode structuré typé :



 */
    object X {
      case class MonJobConfig(config1: String, config2: Int)

      object MonJobConfig {
        def fromConfig(config: com.typesafe.config.Config): Try[MonJobConfig] = ???
      }

      class MonJob(config: MonJobConfig) {
        def superFonction2(df: DataFrame): DataFrame = ???
      }
    }

    /*
      cela permet de valider la configuration du programme avant de démarrer les traitements (via MonJobConfig.fromConfig(...) qui renvoit un Try) et aussi de voir clairement quelle partie du programme utilise tel configuration. (alt+F7 sur IntelliJ, find usages)
 */

    /*
    3 Il manque des encodeurs

    Quand vous allez vouloir générifier du code en spark, il va vous manquer des implicits. Contrairement aux erreurs d'implicit classiques qui consistent à importer ss.implicits._, ici il faut récupérer les implicits manquants.
 */

    // Dans la denière version d'intelliJ, on peut voir les implicits "Show implicit hints"

    /*
    def groupByKeyRemoveNull[A, K](dataset: Dataset[A])(f: A => Option[K]): Dataset[(K, Seq[A])] = {
      dataset
        .flatMap(x => f(x).map(y => (y, x)))
        .groupByKey(_._1)
        .mapGroups({ case (k, it) => (k, it.map(_._2).toSeq) })
    }*/

    /*

    IntelliJ est assez clair sur ce qui manque quand les implicits sont visibles. On va ouvrir un troisième groupe d'argument pour les récupérer
 */

    def groupByKeyRemoveNull[A, K](dataset: Dataset[A])(f: A => Option[K])(
        implicit encKA: Encoder[(K, A)],
        encK: Encoder[K],
        encKSeqA: Encoder[(K, Seq[A])]): Dataset[(K, Seq[A])] = {
      dataset
        .flatMap(x => f(x).map(y => (y, x)))
        .groupByKey(_._1)
        .mapGroups({ case (k, it) => (k, it.map(_._2).toSeq) })
    }

  }

}

package com.typesafe.config {
  trait Config
}

 */
