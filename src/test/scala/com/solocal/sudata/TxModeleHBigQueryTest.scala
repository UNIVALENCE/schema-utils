package com.solocal.sudata

import java.time.LocalDate

import org.scalatest.FunSuite

class TxModeleHBigQueryTest extends FunSuite {

  test("testAllDaysBetween") {


    val d0 = TxModeleHBigQuery.allDaysBetween(LocalDate.parse("2019-10-01"), LocalDate.parse("2019-10-02"))
    assert(d0.size == 2)


    val days = TxModeleHBigQuery.allDaysBetween(LocalDate.parse("2019-10-01"), LocalDate.parse("2019-11-11"))
    assert(days.size == 42)
    assert(days.contains(LocalDate.parse("2019-11-11")))

    assert(days.contains(LocalDate.parse("2019-11-01")))
  }



  ignore("compressGlob") {

    def compressGlob(s:String*):String = ???
    def compressGlobLocalDate(d:LocalDate*):String = ???

    implicit def strToLocalDate(str:String):LocalDate  = LocalDate.parse(str)

    assert(compressGlob("2019-05-01","2019-05-02","2019-06-01") == "2019-0{5-0{1,2},6-01}")

    assert(compressGlobLocalDate("2019-05-01","2019-05-02","2019-05-03","2019-06-01") == "2019-{05-[01-03],06-01}")



  }
}
