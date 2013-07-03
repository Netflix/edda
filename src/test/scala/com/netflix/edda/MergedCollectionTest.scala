/**
 * Copyright 2012 Netflix, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.netflix.edda

import org.slf4j.LoggerFactory

import org.scalatest.FunSuite

import scala.actors.Actor
import scala.concurrent._
import scala.concurrent.duration._
import scala.concurrent.ExecutionContext.Implicits.global

class MergedCollectionTest extends FunSuite {
  import Utils._
  import Queryable._

  def SYNC[T](future: Awaitable[T]): T = {
    Await.result(future, Duration(5, SECONDS))
  }

  val logger = LoggerFactory.getLogger(getClass)
  test("query") {
    val collA = new TestCollection("test.A")
    collA.dataStore.get.records = Seq(Record("a", 1), Record("b", 2), Record("c", 3))
    val collB = new TestCollection("test.B")
    collB.dataStore.get.records = Seq(Record("A", 1), Record("B", 2), Record("C", 3))

    val merged = new MergedCollection("merged.collection", Seq(collA, collB))
    merged.start()

    expectResult(2) {
      SYNC ( merged.query(Map("data" -> 1)) ).size
    }

    expectResult(4) {
      SYNC( merged.query(Map("data" -> Map("$gte" -> 2))) ).size
    }

    expectResult(2) {
      SYNC( merged.query(Map("id" -> Map("$in" -> Seq("A", "a")))) ).size
    }

    merged.stop()
  }
}
