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

import java.util.Properties
import org.joda.time.DateTime
import org.joda.time.DateTimeZone.UTC

import com.netflix.config.DynamicPropertyFactory
import com.netflix.config.ConcurrentCompositeConfiguration
import org.apache.commons.configuration.MapConfiguration

import org.scalatest.FunSuite

class UtilsTest extends FunSuite {

  test("getProperty") {
    DynamicPropertyFactory.getInstance()
    val composite = DynamicPropertyFactory. getBackingConfigurationSource.asInstanceOf[ConcurrentCompositeConfiguration]
    val config = new MapConfiguration(new Properties);
    composite.addConfigurationAtFront(config, "testConfig")

    config.addProperty("edda.other.context.setting", "value1")
    expectResult("default") {
      Utils.getProperty("edda", "setting", "my.context", "default").get
    }

    config.addProperty("edda.setting", "value2")
    expectResult("value2") {
      Utils.getProperty("edda", "setting", "my.context", "default").get
    }

    config.addProperty("edda.context.setting", "value3")
    expectResult("value3") {
      Utils.getProperty("edda", "setting", "my.context", "default").get
    }

    config.addProperty("edda.my.setting", "value4")
    expectResult("value4") {
      Utils.getProperty("edda", "setting", "my.context", "default").get
    }

    config.addProperty("edda.my.context.setting", "value5")
    expectResult("value5") {
      Utils.getProperty("edda", "setting", "my.context", "default").get
    }

  }

  test("toPrettyJson") {
    val date = new DateTime(0, UTC)
    val expected = """{
  "date" : "1970-01-01T00:00:00.000Z",
  "foo" : "bar",
  "list" : [
    1,
    2,
    3,
    4
  ]
}"""
    expectResult(expected) {
      Utils.toPrettyJson(Map("foo" -> "bar", "list" -> List(1, 2, 3, 4), "date" -> date))
    }
  }

  test("writeJson") {
    val date = new DateTime(0, UTC)
    val expected = """{"date":0,"foo":"bar","list":[1,2,3,4]}"""
    expectResult(expected) {
      Utils.toJson(Map("foo" -> "bar", "list" -> List(1, 2, 3, 4), "date" -> date))
    }
    expectResult(expected) {
      Utils.toJson(Map("foo" -> "bar", "list" -> Seq(1, 2, 3, 4), "date" -> date))
    }
    expectResult(expected) {
      Utils.toJson(Map("foo" -> "bar", "list" -> Range(1, 5), "date" -> date))
    }
  }

  test("diffRecords") {
    val r1 = Record("id", 1).copy(stime = new DateTime(0, UTC))
    val r2 = Record("id", 2).copy(stime = new DateTime(1, UTC))
    var expected =
"""--- collection/path/id;_pp;_at=0
+++ collection/path/id;_pp;_at=1
@@ -1,1 +1,1 @@
-1
+2
"""
    expectResult(expected) {
      Utils.diffRecords(Seq(r2, r1), None, "collection/path")
    }

    val r3 = Record("id", List("this", "is", "a", "test")).copy(stime = new DateTime(0, UTC))
    val r4 = Record("id", List("this", "is", "a", "great", "test")).copy(stime = new DateTime(1, UTC))
    val r5 = Record("id", List("this", "is", "an", "even", "better", "test")).copy(stime = new DateTime(2, UTC))

    expected =
"""--- collection/path/id;_pp;_at=1
+++ collection/path/id;_pp;_at=2
@@ -1,7 +1,8 @@
 [
   "this",
   "is",
-  "a",
-  "great",
+  "an",
+  "even",
+  "better",
   "test"
 ]
--- collection/path/id;_pp;_at=0
+++ collection/path/id;_pp;_at=1
@@ -1,6 +1,7 @@
 [
   "this",
   "is",
   "a",
+  "great",
   "test"
 ]
"""
    expectResult(expected) {
      Utils.diffRecords(Seq(r5, r4, r3), None, "collection/path")
    }

    expected =
"""--- collection/path/id;_pp;_at=1
+++ collection/path/id;_pp;_at=2
@@ -4,2 +4,3 @@
-  "a",
-  "great",
+  "an",
+  "even",
+  "better",
--- collection/path/id;_pp;_at=0
+++ collection/path/id;_pp;_at=1
@@ -5,0 +5,1 @@
+  "great",
"""
    expectResult(expected) {
      Utils.diffRecords(Seq(r5, r4, r3), Some(0), "collection/path")
    }
  }
}
