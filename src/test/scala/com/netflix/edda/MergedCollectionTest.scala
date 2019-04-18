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

import com.netflix.config.{ConcurrentCompositeConfiguration, DynamicPropertyFactory}
import org.apache.commons.configuration.MapConfiguration
import org.scalatest.FunSuite

import scala.actors.Actor
import scala.concurrent._
import scala.concurrent.duration._
import scala.concurrent.ExecutionContext.Implicits.global
import org.apache.log4j.Logger
import org.apache.log4j.Level

class MergedCollectionTest extends FunSuite {
  import Utils._
  import Queryable._

  implicit val req = RequestId()

  val logger = Logger.getRootLogger()
  //logger.setLevel(Level.DEBUG)

  def SYNC[T](future: Awaitable[T]): T = {
    Await.result(future, Duration(5, SECONDS))
  }

  test("query") {
    DynamicPropertyFactory.getInstance()
    val composite = DynamicPropertyFactory. getBackingConfigurationSource.asInstanceOf[ConcurrentCompositeConfiguration]
    val config = new MapConfiguration(new Properties);
    composite.addConfigurationAtFront(config, "testConfig")

    config.addProperty("edda.collection.jitter.enabled", "false")

    val collA = new TestCollection("test.A")
    collA.elector.leader = false
    val collB = new TestCollection("test.B")
    collB.elector.leader = false
    val merged = new MergedCollection("merged.collection", Seq(collA, collB))
    
    merged.start()

    collA.dataStore.get.recordSet = collA.dataStore.get.recordSet.copy(records = Seq(Record("a", 1), Record("b", 2), Record("c", 3)))
    collB.dataStore.get.recordSet = collB.dataStore.get.recordSet.copy(records = Seq(Record("A", 1), Record("B", 2), Record("C", 3)))

    SYNC( collA.addObserver(Actor.self) )
    SYNC( collB.addObserver(Actor.self) )
    
    Actor.self receive {
      case Collection.UpdateOK(`collA`, d, meta) => Unit
    }
    Actor.self receive {
      case Collection.UpdateOK(`collB`, d, meta) => Unit
    }

    assertResult(2) {
      SYNC ( merged.query(Map("data" -> 1)) ).size
    }
    
    assertResult(4) {
      SYNC ( merged.query(Map("data" -> Map("$gte" -> 2))) ).size
    }

    assertResult(2) {
      SYNC ( merged.query(Map("id" -> Map("$in" -> Seq("A", "a")))) ).size
    }
    
    SYNC( collA.delObserver(Actor.self) )
    SYNC( collB.delObserver(Actor.self) )
    merged.stop()
  }
}
