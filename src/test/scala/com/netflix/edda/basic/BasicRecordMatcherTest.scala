/**
 * Copyright 2012 Netflix, Inc.
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
package com.netflix.edda.basic

import java.util.Date

import org.joda.time.DateTime

import com.netflix.edda.Record

import org.slf4j.{ Logger, LoggerFactory }

import org.scalatest.FunSuite
import org.scalatest.matchers.MustMatchers

class BasicRecordMatcherTest extends FunSuite with MustMatchers {
    val logger = LoggerFactory.getLogger(getClass)

    test("boolean") {
        val matcher = new BasicRecordMatcher
        val record = Record("id", true).toMap
        
        matcher.doesMatch(Map("data" -> true), record) must be (true)
        matcher.doesMatch(Map("data" -> false), record) must be (false)
        matcher.doesMatch(Map("data" -> "true"), record) must be (true)
        matcher.doesMatch(Map("data" -> "false"), record) must be (false)
    }

    test("time") {
        val matcher = new BasicRecordMatcher

        val record = Record("id", new Date(0)).toMap

        matcher.doesMatch(Map("data" -> new Date(0)), record) must be (true)
        matcher.doesMatch(Map("data" -> new DateTime(0)), record) must be (true)
        matcher.doesMatch(Map("data" -> 0L), record) must be (true)
        matcher.doesMatch(Map("data" -> new Date(1)), record) must be (false)
        matcher.doesMatch(Map("data" -> new DateTime(1)), record) must be (false)
        matcher.doesMatch(Map("data" -> 1L), record)  must be (false)
        
        val dtRecord = Record("id", new DateTime(0)).toMap
     
        matcher.doesMatch(Map("data" -> new Date(0)), dtRecord) must be (true)
        matcher.doesMatch(Map("data" -> new DateTime(0)), dtRecord) must be (true)
        matcher.doesMatch(Map("data" -> 0L), dtRecord) must be (true)
        matcher.doesMatch(Map("data" -> new Date(1)), dtRecord) must be (false)
        matcher.doesMatch(Map("data" -> new DateTime(1)), dtRecord) must be (false)
        matcher.doesMatch(Map("data" -> 1L), dtRecord) must be (false)
    }

    test("string") {
        val matcher = new BasicRecordMatcher
        val record = Record("id", "true").toMap

        // case (found: String, expected: Boolean) => found.toBoolean.compareTo(expected)
        matcher.doesMatch(Map("data" -> true), record) must be (true)
        matcher.doesMatch(Map("data" -> false), record) must be (false)
        matcher.doesMatch(Map("data" -> "true"), record) must be (true)
        matcher.doesMatch(Map("data" -> "false"), record) must be (false)
        
        val numRecord = Record("id", "10").toMap
        // case (found: String, expected) => found.compareTo(expected.toString)
        matcher.doesMatch(Map("data" -> 10), numRecord) must be (true)
        matcher.doesMatch(Map("data" -> 10L), numRecord) must be (true)
        matcher.doesMatch(Map("data" -> "10"), numRecord) must be (true)
        matcher.doesMatch(Map("data" -> "10 "), numRecord) must be (false)
        matcher.doesMatch(Map("data" -> 11), numRecord) must be (false)
        matcher.doesMatch(Map("data" -> 11L), numRecord) must be (false)
        
        val charRecord = Record("id", "C").toMap
        matcher.doesMatch(Map("data" -> 'C'), charRecord) must be (true)

        // case (found: String, expected: Byte) => found.compareTo(expected.toChar.toString)
        matcher.doesMatch(Map("data" -> 'C'.toByte), charRecord) must be (true)

        // case (found, expected: String) => found.toString.compareTo(expected)

        matcher.doesMatch(Map("data" -> "10.0"), Record("id", 10.0F).toMap) must be (true)
        matcher.doesMatch(Map("data" -> "10.0"), Record("id", 10.0D).toMap) must be (true)
        matcher.doesMatch(Map("data" -> "10"), Record("id", 10).toMap) must be (true)
        matcher.doesMatch(Map("data" -> "10"), Record("id", 10L).toMap) must be (true)
        matcher.doesMatch(Map("data" -> "10"), Record("id", 10.toShort).toMap) must be (true)
    }

    test("double") {
        val matcher = new BasicRecordMatcher
        
        matcher.doesMatch(Map("data" -> 1.2345D),     Record("id", 1.2345D).toMap) must be (true)
        matcher.doesMatch(Map("data" -> 0.012345e2D), Record("id", 1.2345D).toMap) must be (true)
        matcher.doesMatch(Map("data" -> 1.2345F),     Record("id", 1.2345D).toMap) must be (true)
        matcher.doesMatch(Map("data" -> 0.012345e2F), Record("id", 1.2345D).toMap) must be (true)

        matcher.doesMatch(Map("data" -> 1L),        Record("id", 1.0D).toMap) must be (true)
        matcher.doesMatch(Map("data" -> 1),         Record("id", 1.0D).toMap) must be (true)
        matcher.doesMatch(Map("data" -> 1.toShort), Record("id", 1.0D).toMap) must be (true)
        matcher.doesMatch(Map("data" -> 1.toByte),  Record("id", 1.0D).toMap) must be (true)
    }

    test("float") {
        val matcher = new BasicRecordMatcher
        
        matcher.doesMatch(Map("data" -> 1.2345D),     Record("id", 1.2345F).toMap) must be (true)
        matcher.doesMatch(Map("data" -> 0.012345e2D), Record("id", 1.2345F).toMap) must be (true)
        matcher.doesMatch(Map("data" -> 1.2345F),     Record("id", 1.2345F).toMap) must be (true)
        matcher.doesMatch(Map("data" -> 0.012345e2F), Record("id", 1.2345F).toMap) must be (true)

        matcher.doesMatch(Map("data" -> 1L),        Record("id", 1.0F).toMap) must be (true)
        matcher.doesMatch(Map("data" -> 1),         Record("id", 1.0F).toMap) must be (true)
        matcher.doesMatch(Map("data" -> 1.toShort), Record("id", 1.0F).toMap) must be (true)
        matcher.doesMatch(Map("data" -> 1.toByte),  Record("id", 1.0F).toMap) must be (true)
    }

    test("long") {
        val matcher = new BasicRecordMatcher

        matcher.doesMatch(Map("data" -> 1D),        Record("id", 1L).toMap) must be (true)
        matcher.doesMatch(Map("data" -> 1F),        Record("id", 1L).toMap) must be (true)
        matcher.doesMatch(Map("data" -> 1L),        Record("id", 1L).toMap) must be (true)
        matcher.doesMatch(Map("data" -> 1),         Record("id", 1L).toMap) must be (true)
        matcher.doesMatch(Map("data" -> 1.toShort), Record("id", 1L).toMap) must be (true)
        matcher.doesMatch(Map("data" -> 1.toByte),  Record("id", 1L).toMap) must be (true)
    }

    test("int") {
        val matcher = new BasicRecordMatcher

        matcher.doesMatch(Map("data" -> 1D),        Record("id", 1).toMap) must be (true)
        matcher.doesMatch(Map("data" -> 1F),        Record("id", 1).toMap) must be (true)
        matcher.doesMatch(Map("data" -> 1L),        Record("id", 1).toMap) must be (true)
        matcher.doesMatch(Map("data" -> 1),         Record("id", 1).toMap) must be (true)
        matcher.doesMatch(Map("data" -> 1.toShort), Record("id", 1).toMap) must be (true)
        matcher.doesMatch(Map("data" -> 1.toByte),  Record("id", 1).toMap) must be (true)
    }

    test("short") {
        val matcher = new BasicRecordMatcher

        matcher.doesMatch(Map("data" -> 1D),        Record("id", 1.toShort).toMap) must be (true)
        matcher.doesMatch(Map("data" -> 1F),        Record("id", 1.toShort).toMap) must be (true)
        matcher.doesMatch(Map("data" -> 1L),        Record("id", 1.toShort).toMap) must be (true)
        matcher.doesMatch(Map("data" -> 1),         Record("id", 1.toShort).toMap) must be (true)
        matcher.doesMatch(Map("data" -> 1.toShort), Record("id", 1.toShort).toMap) must be (true)
        matcher.doesMatch(Map("data" -> 1.toByte),  Record("id", 1.toShort).toMap) must be (true)
    }

    test("char") {
        val matcher = new BasicRecordMatcher

        matcher.doesMatch(Map("data" -> 1L),         Record("id", '1').toMap) must be (true)
        matcher.doesMatch(Map("data" -> 1),          Record("id", '1').toMap) must be (true)
        matcher.doesMatch(Map("data" -> 1.toShort),  Record("id", '1').toMap) must be (true)
        matcher.doesMatch(Map("data" -> '1'),        Record("id", '1').toMap) must be (true)
        matcher.doesMatch(Map("data" -> '1'.toByte), Record("id", '1').toMap) must be (true)
    }

    test("byte") {
        val matcher = new BasicRecordMatcher

        matcher.doesMatch(Map("data" -> 49L),        Record("id", '1'.toByte).toMap) must be (true)
        matcher.doesMatch(Map("data" -> 49),         Record("id", '1'.toByte).toMap) must be (true)
        matcher.doesMatch(Map("data" -> 49.toShort), Record("id", '1'.toByte).toMap) must be (true)
        matcher.doesMatch(Map("data" -> '1'),        Record("id", '1'.toByte).toMap) must be (true)
        matcher.doesMatch(Map("data" -> '1'.toByte), Record("id", '1'.toByte).toMap) must be (true)
    }

    test("null") {
        val matcher = new BasicRecordMatcher
        
        matcher.doesMatch(Map("data" -> null),      Record("id", null).toMap) must be (true)
        // key is explicitly set to null
        matcher.doesMatch(Map("data.key" -> null),  Record("id", Map("key" -> null)).toMap) must be (true)
        // key is explicitly non null
        matcher.doesMatch(Map("data.key" -> null),  Record("id", Map("key" -> true)).toMap) must be (false)
        // key is completly missing
        matcher.doesMatch(Map("data.key" -> null),  Record("id", Map("bar" -> null)).toMap) must be (false)
    }

    test("map") {
        val matcher = new BasicRecordMatcher
        
        matcher.doesMatch(Map("data.a" -> 1),    Record("id", Map("a" -> 1, "b" -> 2, "c" -> 3)).toMap) must be (true)
        matcher.doesMatch(Map("data.b" -> 2),    Record("id", Map("a" -> 1, "b" -> 2, "c" -> 3)).toMap) must be (true)
        matcher.doesMatch(Map("data.c" -> 3),    Record("id", Map("a" -> 1, "b" -> 2, "c" -> 3)).toMap) must be (true)
        matcher.doesMatch(Map("data.d" -> 1),    Record("id", Map("a" -> 1, "b" -> 2, "c" -> 3)).toMap) must be (false)

        matcher.doesMatch(Map("data.a.x" -> 1), Record("id", Map("a" -> Map("x" -> 1, "y" -> 2))).toMap) must be (true)
    }

    test("list") {
        val matcher = new BasicRecordMatcher

        matcher.doesMatch(Map("data" -> 1),    Record("id", List(1,2,3)).toMap) must be (true)
        matcher.doesMatch(Map("data" -> 2),    Record("id", List(1,2,3)).toMap) must be (true)
        matcher.doesMatch(Map("data" -> 3),    Record("id", List(1,2,3)).toMap) must be (true)
        matcher.doesMatch(Map("data" -> 4),    Record("id", List(1,2,3)).toMap) must be (false)

        // when 2 lists are compared we want to check if the expected result is a subset of the record value
        matcher.doesMatch(Map("data" -> List(1)),    Record("id", List(1,2,3)).toMap) must be (true)
        matcher.doesMatch(Map("data" -> List(1,2)),    Record("id", List(1,2,3)).toMap) must be (true)
        // ordering is irrelevent
        matcher.doesMatch(Map("data" -> List(2,1)),    Record("id", List(1,2,3)).toMap) must be (true)
        matcher.doesMatch(Map("data" -> List(1,2,3)),    Record("id", List(1,2,3)).toMap) must be (true)
        // 4 is not in list
        matcher.doesMatch(Map("data" -> List(1,2,3,4)),    Record("id", List(1,2,3)).toMap) must be (false)
        // again 4 is not in list
        matcher.doesMatch(Map("data" -> List(4)),    Record("id", List(1,2,3)).toMap) must be (false)
    }

    test("$or") {
        val matcher = new BasicRecordMatcher

        matcher.doesMatch(Map("$or" -> Seq(Map("data" -> 3), Map("data" -> 4))), Record("id", 4).toMap) must be (true)
        matcher.doesMatch(Map("$or" -> Seq(Map("data" -> 3), Map("data" -> 5))), Record("id", 4).toMap) must be (false)
    }

    test("$and") {
        val matcher = new BasicRecordMatcher

        matcher.doesMatch(Map("$and" -> Seq(Map("data" -> 4), Map("data" -> "4"))), Record("id", 4).toMap) must be (true)
        matcher.doesMatch(Map("$and" -> Seq(Map("data" -> 4), Map("data" -> "5"))), Record("id", 4).toMap) must be (false)
    }

    test("$eq") {
        val matcher = new BasicRecordMatcher

        matcher.doesMatch(Map("data" -> Map("$eq" -> 4)),     Record("id", 4).toMap) must be (true)
        matcher.doesMatch(Map("data" -> Map("$eq" -> "4")),   Record("id", 4).toMap) must be (true)
        matcher.doesMatch(Map("data" -> Map("$eq" -> "foo")), Record("id", 4).toMap) must be (false)
    }

    test("$ne") {
        val matcher = new BasicRecordMatcher

        matcher.doesMatch(Map("data" -> Map("$ne" -> 4)),     Record("id", 4).toMap) must be (false)
        matcher.doesMatch(Map("data" -> Map("$ne" -> "4")),   Record("id", 4).toMap) must be (false)
        matcher.doesMatch(Map("data" -> Map("$ne" -> "foo")), Record("id", 4).toMap) must be (true)
    }
    
    test("$gt") {
        val matcher = new BasicRecordMatcher

        matcher.doesMatch(Map("data" -> Map("$gt" -> 3)),   Record("id", 4).toMap) must be (true)
        matcher.doesMatch(Map("data" -> Map("$gt" -> "4")), Record("id", 4).toMap) must be (false)
        matcher.doesMatch(Map("data" -> Map("$gt" -> 4)),   Record("id", 4).toMap) must be (false)

        matcher.doesMatch(Map("data" -> Map("$gt" -> "a")),   Record("id", "b").toMap) must be (true)
        matcher.doesMatch(Map("data" -> Map("$gt" -> "c")),   Record("id", "b").toMap) must be (false)
    }

    test("$gte") {
        val matcher = new BasicRecordMatcher

        matcher.doesMatch(Map("data" -> Map("$gte" -> 3)),   Record("id", 4).toMap) must be (true)
        matcher.doesMatch(Map("data" -> Map("$gte" -> "4")), Record("id", 4).toMap) must be (true)
        matcher.doesMatch(Map("data" -> Map("$gte" -> 4)),   Record("id", 4).toMap) must be (true)

        matcher.doesMatch(Map("data" -> Map("$gte" -> "a")),   Record("id", "b").toMap) must be (true)
        matcher.doesMatch(Map("data" -> Map("$gte" -> "b")),   Record("id", "b").toMap) must be (true)
        matcher.doesMatch(Map("data" -> Map("$gte" -> "c")),   Record("id", "b").toMap) must be (false)
    }

    test("$lt") {
        val matcher = new BasicRecordMatcher

        matcher.doesMatch(Map("data" -> Map("$lt" -> 5)),   Record("id", 4).toMap) must be (true)
        matcher.doesMatch(Map("data" -> Map("$lt" -> "4")), Record("id", 4).toMap) must be (false)
        matcher.doesMatch(Map("data" -> Map("$lt" -> 4)),   Record("id", 4).toMap) must be (false)

        matcher.doesMatch(Map("data" -> Map("$lt" -> "c")),   Record("id", "b").toMap) must be (true)
        matcher.doesMatch(Map("data" -> Map("$lt" -> "a")),   Record("id", "b").toMap) must be (false)
    }

    test("$lte") {
        val matcher = new BasicRecordMatcher

        matcher.doesMatch(Map("data" -> Map("$lte" -> 5)),   Record("id", 4).toMap) must be (true)
        matcher.doesMatch(Map("data" -> Map("$lte" -> "4")), Record("id", 4).toMap) must be (true)
        matcher.doesMatch(Map("data" -> Map("$lte" -> 4)),   Record("id", 4).toMap) must be (true)

        matcher.doesMatch(Map("data" -> Map("$lte" -> "c")),   Record("id", "b").toMap) must be (true)
        matcher.doesMatch(Map("data" -> Map("$lte" -> "b")),   Record("id", "b").toMap) must be (true)
        matcher.doesMatch(Map("data" -> Map("$lte" -> "a")),   Record("id", "b").toMap) must be (false)
    }

    test("$exists") {
        val matcher = new BasicRecordMatcher

        matcher.doesMatch(Map("data.foo" -> Map("$exists" -> true)),  Record("id", Map("foo"-> null, "baz" -> false)).toMap) must be (true)
        matcher.doesMatch(Map("data.foo" -> Map("$exists" -> false)), Record("id", Map("foo"-> null, "baz" -> false)).toMap) must be (false)
        matcher.doesMatch(Map("data.bar" -> Map("$exists" -> true)),  Record("id", Map("foo"-> null, "baz" -> false)).toMap) must be (false)
        matcher.doesMatch(Map("data.bar" -> Map("$exists" -> false)), Record("id", Map("foo"-> null, "baz" -> false)).toMap) must be (true)
        matcher.doesMatch(Map("data.baz" -> Map("$exists" -> true)),  Record("id", Map("foo"-> null, "baz" -> false)).toMap) must be (true)
        matcher.doesMatch(Map("data.baz" -> Map("$exists" -> false)), Record("id", Map("foo"-> null, "baz" -> false)).toMap) must be (false)
    }

    test("$in") {
        val matcher = new BasicRecordMatcher

        matcher.doesMatch(Map("data" -> Map("$in" -> Seq(3,4,5))), Record("id", 4).toMap) must be (true)
        matcher.doesMatch(Map("data" -> Map("$in" -> Seq(3,5))),   Record("id", 4).toMap) must be (false)
    }

    test("$nin") {
        val matcher = new BasicRecordMatcher

        matcher.doesMatch(Map("data" -> Map("$nin" -> Seq(3,4,5))), Record("id", 4).toMap) must be (false)
        matcher.doesMatch(Map("data" -> Map("$nin" -> Seq(3,5))),   Record("id", 4).toMap) must be (true)
    }

    test("$regex") {
        val matcher = new BasicRecordMatcher
        
        matcher.doesMatch(Map("data" -> Map("$regex" -> "^test")), Record("id", "testing 123").toMap) must be (true)
        matcher.doesMatch(Map("data" -> Map("$regex" -> "123$")), Record("id", "testing 123").toMap) must be (true)
        matcher.doesMatch(Map("data" -> Map("$regex" -> "testing.123")), Record("id", "testing 123").toMap) must be (true)
        matcher.doesMatch(Map("data" -> Map("$regex" -> "^[a-z]+ \\d+$")), Record("id", "testing 123").toMap) must be (true)
        matcher.doesMatch(Map("data" -> Map("$regex" -> "^Test")), Record("id", "testing 123").toMap) must be (false)
        matcher.doesMatch(Map("data" -> Map("$regex" -> "^(?i)TESTING")), Record("id", "testing 123").toMap) must be (true)
    }
}
