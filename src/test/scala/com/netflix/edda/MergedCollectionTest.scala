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
package com.netflix.edda

import org.slf4j.{ Logger, LoggerFactory }

import com.netflix.edda.basic.BasicContext

import org.scalatest.FunSuite

class MergedCollectionTest extends FunSuite {
    val logger = LoggerFactory.getLogger(getClass)
    test("query") {
        val collA = new TestCollection
        collA.datastore.get.records = Seq(Record("a", 1), Record("b", 2), Record("c",3))
        val collB = new TestCollection
        collB.datastore.get.records = Seq(Record("A", 1), Record("B", 2), Record("C",3))
        
        val merged = new MergedCollection("merged.collection", Seq(collA, collB))
        merged.start
        
        expect(2) {
            merged.query(Map("data" -> 1)).size
        }
        
        expect(4) {
            merged.query(Map("data" -> Map("$gte" -> 2))).size
        }

        expect(2) {
            merged.query(Map("id" -> Map("$in" -> Seq("A", "a")))).size
        }
    }
}
