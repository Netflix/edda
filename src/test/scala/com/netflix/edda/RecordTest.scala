/*
 * Copyright 2012-2019 Netflix, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.netflix.edda

import org.joda.time.DateTime

import org.scalatest.FunSuite

class RecordTest extends FunSuite {
  test("Apply") {

    val date = new DateTime(1)
    val ftime = date
    val ctime = date
    val stime = date
    val ltime = null
    val mtime = date
    val data = 1
    val tags = Map[String, Any]()

    val expected = new Record("id", ftime, ctime, stime, ltime, mtime, data, tags)
    val expectedJson = expected.toString

    expectResult(expectedJson) {
      Record("id", 1).copy(ftime = date, ctime = date, stime = date, mtime = date).toString
    }

    expectResult(true) {
      expected.sameData(Record("id", 1).copy(ftime = date, ctime = date, stime = date, mtime = date))
    }

    expectResult(expectedJson) {
      Record("id", date, 1).copy(ftime = date, stime = date, mtime = date).toString
    }

    expectResult(true) {
      expected.sameData(Record("id", date, 1).copy(ftime = date, stime = date, mtime = date))
    }

    expectResult(expectedJson) {
      Record("id", date, date, date, null, date, 1, Map()).toString
    }

    expectResult(true) {
      expected.sameData(Record("id", date, date, date, null, date, 1, Map()))
    }
  }

  test("toMap") {
    val date = new DateTime(1)
    val rec = Record("id", date, date, date, null, date, 1, Map())
    val map = Map("id" -> "id", "ftime" -> date, "ctime" -> date, "stime" -> date, "ltime" -> null, "mtime" -> date, "data" -> 1, "tags" -> Map())
    expectResult(map) {
      rec.toMap
    }
  }

  test("dataString") {
    expectResult("1") {
      Record("id", 1).dataString
    }

    expectResult( """{"foo":"bar"}""") {
      Record("id", Map("foo" -> "bar")).dataString
    }
  }
}
