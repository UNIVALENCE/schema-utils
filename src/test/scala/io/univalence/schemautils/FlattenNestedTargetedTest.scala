package io.univalence.schemautils

import org.apache.spark.sql.SaveMode
import org.scalatest.FunSuiteLike

class FlattenNestedTargetedTest extends SparkTest with FunSuiteLike {

  test("add sum-up") {

    val in = dfFromJson("""
             { idvisitor: 1,
               xxx: {visites:  [
                 {idvisite : 2, recherches: [{idrecherche:3}, {idrecherche:4}] },
                 {idvisite: 3},
                 {idvisite: 5, recherches: [{idrecherche:6}]},
                 {idvisite: 7, recherches: []}
               ] }
             }""")

    val out = dfFromJson("""
   {
  "idvisitor": 1,
  "xxx": {
    "visites": [
      {
        "idvisite": 2,
        "recherches": [
          {
            "idrecherche": 3
          },
          {
            "idrecherche": 4
          }
        ],
        "nbrecherches": 2
      },
      {
        "idvisite": 3,
        "nbrecherches": -1
      },
      {
        "idvisite": 5,
        "recherches": [
          {
            "idrecherche": 6
          }
        ],
        "nbrecherches": 1
      },
      {
        "idvisite": 7,
        "recherches": [],
        "nbrecherches": 0
      }
    ]
  }
}
    """)

    val extraField: String => (SingleExp, String) = str => SingleExp(s"cardinality($str.recherches)") -> "nbrecherches"

    import FlattenNestedTargeted._

    //path"xxx.visites/"
    val res = addFieldAtPath(Path.select.xxx.visites.>,
                             (_, point) => (SingleExp(s"cardinality($point.recherches)"), "nbrecherches"))(in)

    assertDfEqual(res, out)
  }

  test("add_link") {
    val in = dfFromJson("""
             { idvisitor: 1,
               xxx: {visites:  [
                 {idvisite : 2, recherches: [{idrecherche:3}, {idrecherche:4}] },
                 {idvisite: 3},
                 {idvisite: 5, recherches: [{idrecherche:6}]},
                 {idvisite: 7, recherches: []}
               ] }
             }""")

    val out1 = dfFromJson("""
      {
         idvisitor:1,
         xxx: {visites:[
            {idvisite:2, recherches_link: [1,2]},
            {idvisite:3},
            {idvisite:5, recherches_link: [4]},
            {idvisite:7, recherches_link: []}
            ],
         recherches:[{visite_idvisite:2,idrecherche:3},
                     {visite_idvisite:2,idrecherche:4},
                     {visite_idvisite:3},
                     {visite_idvisite:5,idrecherche:6},
                     {visite_idvisite:7}]
      }}
    """)

    val out2 = dfFromJson("""
      {
         idvisitor:1,
         xxx: {visites:[
            {idvisite:2, recherches_link: [1,2]},
            {idvisite:3},
            {idvisite:5, recherches_link: [3]},
            {idvisite:7, recherches_link: []}
            ],
         recherches:[{visite_idvisite:2,idrecherche:3},
                     {visite_idvisite:2,idrecherche:4},
                     {visite_idvisite:5,idrecherche:6}
                     ]
      }}
    """)

    assertDfEqual(
      FlattenNestedTargeted.detach(
        in,
        target      = Path.select.xxx.visites.>.recherches,
        fieldname   = _.mkString("_"),
        includeRoot = x => Some(("visite" +: x).mkString("_")),
        addLink     = true,
        outer       = true
      ),
      out1
    )

    assertDfEqual(
      FlattenNestedTargeted.detach(
        in,
        target      = Path.select.xxx.visites.>.recherches,
        fieldname   = _.mkString("_"),
        includeRoot = x => Some(("visite" +: x).mkString("_")),
        addLink     = true,
        outer       = false
      ),
      out2
    )
  }

  test("detach outer") {
    val in = dfFromJson("""
             { idvisitor: 1,
               xxx: {visites:  [
                 {idvisite : 2, recherches: [{idrecherche:3}, {idrecherche:4}] },
                 {idvisite: 3},
                 {idvisite: 5, recherches: [{idrecherche:6}]}
               ] }
             }""")

    val out = dfFromJson("""
      {
         idvisitor:1,
         xxx: {visites:[
            {idvisite:2},
            {idvisite:3},
            {idvisite:5}],
         recherches:[{visite_idvisite:2,idrecherche:3},
                     {visite_idvisite:2,idrecherche:4},
                     {visite_idvisite:3},
                     {visite_idvisite:5,idrecherche:6}]
      }}
    """)

    assertDfEqual(
      FlattenNestedTargeted.detach(
        in,
        target      = Path.select.xxx.visites.>.recherches,
        fieldname   = _.mkString("_"),
        includeRoot = x => Some(("visite" +: x).mkString("_")),
        addLink     = false,
        outer       = true
      ),
      out
    )
  }

  test("detach deep") {
    val in = dfFromJson("""
         { idvisitor: 1,
           visites : [
             {idvisite: 2,
              recherches: [
                {idrecherche:3, history: [{idrq:4}]}
             ]}
           ]
           }
      """)

    val out = dfFromJson(
      """{"idvisitor":1,"visites":[{"idvisite":2,"recherches":[{"idrecherche":3}],"history":[{"recherche_idrecherche":3,"idrq":4}]}]}""")

    val res = FlattenNestedTargeted.detach(
      in,
      target      = Path.select.visites.>.recherches.>.history,
      fieldname   = _.mkString("_"),
      includeRoot = x => Some(("recherche" +: x).mkString("_")),
      addLink     = false,
      outer       = false
    )

    assertDfEqual(res, out)
  }

  test("detach 1") {
    val in = dfFromJson("""
             { idvisitor: 1,
               xxx: {visites:  [
               {idvisite : 2, recherches: [{idrecherche:3}, {idrecherche:4}] },
               {idvisite: 3},
               {idvisite: 5, recherches: [{idrecherche:6}]}
               ]}
             }""")

    val out = dfFromJson("""
      {
         idvisitor:1,
         xxx: {visites:[
            {idvisite:2},
            {idvisite:3},
            {idvisite:5}],
         recherches:[{visite_idvisite:2,idrecherche:3},
                     {visite_idvisite:2,idrecherche:4},
                     {visite_idvisite:5,idrecherche:6}]
      }}
    """)

    assertDfEqual(
      FlattenNestedTargeted.detach(
        in,
        target      = Path.select.xxx.visites.>.recherches,
        fieldname   = _.mkString("_"),
        includeRoot = x => Some(("visite" +: x).mkString("_")),
        addLink     = false,
        outer       = false
      ),
      out
    )
  }

  test("toto") {
    val df = dfFromJson("""
             { a: 1,
               b: [{ c: [{ d: 4 },
                         { d: 5 }],
                     e: 6 },
                   { f: 7 }]
             }""")

    //df.printSchema()
    /* root
       |-- a: long (nullable = true)
       |-- b: array (nullable = true)
       |    |-- element: struct (containsNull = true)
       |    |    |-- c: array (nullable = true)
       |    |    |    |-- element: struct (containsNull = true)
       |    |    |    |    |-- d: long (nullable = true)
       |    |    |-- e: long (nullable = true)
       |    |    |-- f: long (nullable = true)
     */

    df.schema.json

    FlattenNestedTargeted.allPaths(df.schema).foreach(println)

    import org.apache.spark.sql.functions._
    println(
      df.select(
          expr("a"),
          expr("""
              flatten(transform(b, v1 ->
                if(
                  cardinality(v1.c) = 0 or v1.c is null,
                  array(named_struct('e',v1.e,'c', cast(null as struct<d:integer>), 'f', v1.f)),
                  transform(v1.c, v2 ->named_struct('e',v1.e, 'c', named_struct('d', v2.d), 'f', v1.f))
                  ))) as b  """)
        )
        .toJSON
        .head())

    /*
    { "a": 1,
      "b": [{ "e": 6,
              "c": { "d": 4 } },
            { "e": 6,
              "c": { "d": 5 } },
            { "f": 7 }] }
   */
  }

  test("modeleH2") {

    val df = dfFromJson("""
             { idvisitor: 1,
               visites:  [
               {idvisite : 2, recherches: [{idrecherche:3}, {idrecherche:4}] },
               {idvisite: 3},
               {idvisite: 5, recherches: [{idrecherche:6}]}
               ]
             }""")

    val out =
      FlattenNestedTargeted.detach(df, Path.select.visites.>.recherches, _.mkString("_"), x => Some(x.mkString("_")))

    out.printSchema()

    out.write.mode(SaveMode.Overwrite).parquet("target/tmpdata/modeleH2")

  }

  test("modeleH") {
    val df = dfFromJson("""
             { idvisitor: 1,
               visites:  [
               {idvisite : 2, recherches: [{idrecherche:3}, {idrecherche:4}] },
               {idvisite: 3},
               {idvisite: 5, recherches: [{idrecherche:6}]}
               ]
             }""")

    """
      {
         "idvisitor":1,
         "visites":[
            {"idvisite":2,"recherches_link":[0,1]},
            {"idvisite":3,},
            {"idvisite":5,"recherches_link":[3]}
         ],
         "recherches":[{"visite_idvisite":2,"idrecherche":3},
                       {"visite_idvisite":2,"idrecherche":4},
                       {"visite_idvisite":3},
                       {"visite_idvisite":5,"idrecherche":6}]}

    """.stripMargin

    df.createTempView("modeleh")

    val dfres = ss.sql(
      """
           select idvisitor,


           zip_with(
            aggregate(visites,
            named_struct('a',cast(array() as array<int>),'b',0),
            (acc, visite) -> named_struct('a',array_union(acc.a, array(acc.b)), 'b', acc.b + array_max(array(cardinality(visite.recherches),1))) , x -> x.a),
            visites,
            (n,visite) -> named_struct('idvisite', visite.idvisite,  'recherches_link',

            if(visite.recherches is null or cardinality(visite.recherches) = 0,
               cast(null as array<int>),
               sequence(n,n + cardinality(visite.recherches) - 1)))) as visites

            ,

             flatten(transform(visites, visite -> transform(
             coalesce(visite.recherches,array(named_struct('idrecherche', cast(null as int)))  ),
             recherche -> named_struct('visite_idvisite', visite.idvisite, 'idrecherche', recherche.idrecherche)))) as recherches

             from modeleh

          """.stripMargin)
    val res =
      dfres.toJSON.head()

    println(res)

    dfres.createTempView("modelehflat")

    ss.sql("""select
         visite.idvisite,
         transform(visite.recherches_link,
         link -> element_at(recherches,link + 1)) as recherches
         from (select recherches, explode(visites) as visite from modelehflat) as in""").toJSON.show(false)

    //df.printSchema()
    /* root
       |-- a: long (nullable = true)
       |-- b: array (nullable = true)
       |    |-- element: struct (containsNull = true)
       |    |    |-- c: array (nullable = true)
       |    |    |    |-- element: struct (containsNull = true)
       |    |    |    |    |-- d: long (nullable = true)
       |    |    |-- e: long (nullable = true)
       |    |    |-- f: long (nullable = true)
     */

    df.schema.json

    FlattenNestedTargeted.allPaths(df.schema).foreach(println)
    /*
    import org.apache.spark.sql.functions._
    println(
      df.select(expr("a"),
        expr("""
              flatten(transform(b, v1 ->
                if(
                  cardinality(v1.c) = 0 or v1.c is null,
                  array(named_struct('e',v1.e,'c', cast(null as struct<d:integer>), 'f', v1.f)),
                  transform(v1.c, v2 ->named_struct('e',v1.e, 'c', named_struct('d', v2.d), 'f', v1.f))
                  ))) as b  """))
        .toJSON
        .head())

     */

    /*
    { "a": 1,
      "b": [{ "e": 6,
              "c": { "d": 4 } },
            { "e": 6,
              "c": { "d": 5 } },
            { "f": 7 }] }
   */
  }

}
