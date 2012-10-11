package com.netflix.edda

import org.joda.time.DateTime

import org.scalatest.FunSuite

class RecordTest extends FunSuite {
    test("Apply") {
        
        val date = new DateTime(1)
        val ctime = date
        val stime = date
        val ltime = null
        val mtime = date
        val data = 1;
        val tags = Map[String,Any]()
        
        val expected = new Record("id",ctime,stime,ltime,mtime,data,tags)
        val expectedJson = expected.toString

        expect(expectedJson) {
            Record("id", 1).copy(ctime=date,stime=date,mtime=date).toString
        }

        expect(true) {
            expected.sameData(Record("id", 1).copy(ctime=date,stime=date,mtime=date))
        }
        
        expect(expectedJson) {
            Record("id", date, 1).copy(stime=date,mtime=date).toString
        }

        expect(true) {
            expected.sameData(Record("id", date, 1).copy(stime=date,mtime=date))
        }

        expect(expectedJson) {
            Record("id",date,date,null,date,1,Map()).toString
        }

        expect(true) {
            expected.sameData(Record("id",date,date,null,date,1,Map()))
        }
    }

    test("toMap") {
        val date = new DateTime(1)
        val rec = Record("id",date,date,null,date,1,Map())
        val map = Map("id" -> "id", "ctime" -> date, "stime" -> date, "ltime" -> null, "mtime" -> date, "data" -> 1, "tags" -> Map())
        expect(map) {
            rec.toMap
        }
    }

    test("dataString") {
        expect("1") {
            Record("id",1).dataString
        }
        
        expect("""{"foo":"bar"}""") {
            Record("id",Map("foo" -> "bar")).dataString
        }
    }
}
