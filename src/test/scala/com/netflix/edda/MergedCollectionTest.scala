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
